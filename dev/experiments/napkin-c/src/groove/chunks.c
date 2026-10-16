/* groove::chunks (crates/groove/src/chunks.rs), non-test part.
 *
 * Not ported (no browser-build rows): MemoryChunkStorage, TestChunkProvider,
 * TestChunkProviderControl. The forwarding `impl ChunkStorage for Rc<S>` is
 * the same groove_DynChunkStorage handle in C.
 *
 * async -> sync: a provider future always completes on its first poll, so
 * the coalesced in-flight registry only ever observes a missing future when a
 * request re-enters itself (ChunkError::Reentrant), and admission while a
 * reclamation guard is held (where Rust stays Pending) reports Reentrant too. */
#include "jz_rt.h"
#include "chunks.h"
#include "_ext_groove-storage.h"

/* ======================================================================
 * Weak<T> over jz_rc (jz_rc_hdr is the public header layout in jz_rt.h)
 * ====================================================================== */
#define RC_HDR_SIZE ((sizeof(jz_rc_hdr) + 7) & ~(usize)7)
static jz_rc_hdr *rc_hdr(const void *p) { return (jz_rc_hdr *)((u8 *)p - RC_HDR_SIZE); }
static void *rc_downgrade(const void *p) {
    rc_hdr(p)->weak++;
    return (void *)p;
}
static void *rc_upgrade(void *weak) { /* Weak::upgrade */
    if (!weak || rc_hdr(weak)->strong == 0) return NULL;
    return jz_rc_clone(weak);
}
static void rc_weak_release(void *weak) {
    if (!weak) return;
    jz_rc_hdr *h = rc_hdr(weak);
    if (--h->weak == 0) jz_free(h);
}

/* ======================================================================
 * Errors
 * ====================================================================== */
jz_err groove__chunks__ChunkStorageError__unavailable(void) { return jz_err_new(GROOVE_CHUNK_STORAGE_E_Unavailable, "chunk is unavailable"); }
jz_err groove__chunks__ChunkStorageError__locator_conflict(void) {
    return jz_err_new(GROOVE_CHUNK_STORAGE_E_LocatorConflict, "opaque locator already names different content");
}
jz_err groove__chunks__ChunkStorageError__integrity(void) {
    return jz_err_new(GROOVE_CHUNK_STORAGE_E_Integrity, "staged chunk failed integrity validation");
}
jz_err groove__chunks__ChunkStorageError__backend(const char *p, u32 n) {
    return jz_err_fmt(GROOVE_CHUNK_STORAGE_E_Backend, "chunk storage failed: %.*s", (int)n, p);
}
jz_err groove__chunks__ChunkError__unavailable(void) { return jz_err_new(GROOVE_CHUNK_E_Unavailable, "chunk is unavailable"); }
jz_err groove__chunks__ChunkError__retryable(u32 retry_after_ms) {
    return jz_err_fmt(GROOVE_CHUNK_E_Retryable, "chunk retrieval is retryable after %ums", (unsigned)retry_after_ms);
}
jz_err groove__chunks__ChunkError__backend(const char *p, u32 n) {
    return jz_err_fmt(GROOVE_CHUNK_E_Backend, "chunk retrieval failed: %.*s", (int)n, p);
}
jz_err groove__chunks__ChunkError__integrity(void) {
    return jz_err_new(GROOVE_CHUNK_E_Integrity, "chunk bytes do not match the requested object hash");
}
jz_err groove__chunks__ChunkError__reentrant(void) {
    return jz_err_new(GROOVE_CHUNK_E_Reentrant, "chunk request re-entered while its backing request was being polled");
}
jz_err groove__chunks__ChunkError__From__from(jz_err e) {
    switch (e.code) {
    case 0: return e;
    case GROOVE_CHUNK_STORAGE_E_Unavailable: jz_err_drop(&e); return groove__chunks__ChunkError__unavailable();
    case GROOVE_CHUNK_STORAGE_E_Integrity: jz_err_drop(&e); return groove__chunks__ChunkError__integrity();
    default: {
        /* Self::Backend(error.to_string()) */
        jz_err out = e.msg ? groove__chunks__ChunkError__backend(e.msg->ptr, e.msg->len) : groove__chunks__ChunkError__backend("", 0);
        jz_err_drop(&e);
        return out;
    }
    }
}
/* parses the {retry_after_ms} back out of a Retryable error */
u32 groove__chunks__ChunkError__retry_after_ms(const jz_err *e) {
    if (e->code != GROOVE_CHUNK_E_Retryable || !e->msg) return 0;
    static const char PREFIX[] = "chunk retrieval is retryable after ";
    u32 n = (u32)sizeof(PREFIX) - 1, v = 0;
    for (u32 i = n; i < e->msg->len && e->msg->ptr[i] >= '0' && e->msg->ptr[i] <= '9'; i++) v = v * 10 + (u32)(e->msg->ptr[i] - '0');
    return v;
}
jz_err groove__chunks__ChunkError__Clone__clone(const jz_err *e) {
    if (!e->code) return JZ_OK;
    if (!e->msg) return jz_err_new(e->code, NULL);
    return jz_err_fmt(e->code, "%.*s", (int)e->msg->len, e->msg->ptr);
}
/* map_err(|error| ChunkStorageError::Backend(error.to_string())) */
static jz_err storage_backend_error(jz_err e) {
    jz_err out = e.msg ? groove__chunks__ChunkStorageError__backend(e.msg->ptr, e.msg->len) : groove__chunks__ChunkStorageError__backend("", 0);
    jz_err_drop(&e);
    return out;
}

/* ======================================================================
 * ChunkRequest
 * ====================================================================== */
int groove__chunks__ChunkRequest__Ord__cmp(const void *a, const void *b, void *ctx) {
    const groove_ChunkRequest *x = (const groove_ChunkRequest *)a, *y = (const groove_ChunkRequest *)b;
    int c = memcmp(x->object_hash, y->object_hash, 32);
    return c ? c : memcmp(x->locator.b, y->locator.b, sizeof(x->locator.b));
}
bool groove__chunks__ChunkRequest__PartialEq__eq(const groove_ChunkRequest *a, const groove_ChunkRequest *b) {
    return groove__chunks__ChunkRequest__Ord__cmp(a, b, NULL) == 0;
}
static bool hash_eq(const groove_ContentHash *a, const groove_ContentHash *b) { return memcmp(a->b, b->b, 32) == 0; }
static groove_ContentHash hash_of(const u8 *p, u32 n) { return groove__large_values__object_hash(p, n); }

static void staged_chunk_drop(void *p) { jz_bytes_drop(&((groove_StagedChunk *)p)->encoded); }
static void staged_chunks_drop(groove_StagedChunkVec *v) { jz_vec_drop(v, sizeof(groove_StagedChunk), staged_chunk_drop); }

/* ======================================================================
 * ChunkStorage dispatch + Rc handle helpers
 * ====================================================================== */
jz_err groove__chunks__ChunkStorage__get(const groove_DynChunkStorage *s, groove_Locator locator, groove_ContentHash expected_hash, jz_bytes *out) {
    return s->vt->get(s->self, locator, expected_hash, out);
}
jz_err groove__chunks__ChunkStorage__stage(const groove_DynChunkStorage *s, groove_StagedChunkVec chunks, groove_StagedLargeValueAccounting *out) {
    return s->vt->stage(s->self, chunks, out);
}
jz_err groove__chunks__ChunkStorage__delete(const groove_DynChunkStorage *s, groove_Locator locator, groove_ContentHash expected_hash) {
    if (s->vt->delete_) return s->vt->delete_(s->self, locator, expected_hash);
    static const char MSG[] = "chunk deletion is not implemented by this backend";
    return groove__chunks__ChunkStorageError__backend(MSG, sizeof(MSG) - 1);
}
groove_DynChunkStorage groove__chunks__DynChunkStorage__clone(const groove_DynChunkStorage *s) {
    groove_DynChunkStorage c = {jz_rc_clone(s->self), s->vt};
    return c;
}
void groove__chunks__DynChunkStorage__drop(groove_DynChunkStorage *s) {
    jz_rc_release(s->self);
    s->self = NULL;
}

/* ======================================================================
 * ManagedChunkStorage
 * ====================================================================== */
typedef struct ManagedChunkStorage { groove_DynChunkKvStorage backend; } ManagedChunkStorage;
static void managed_drop(void *p) { jz_rc_release(((ManagedChunkStorage *)p)->backend.self); }

groove_DynChunkStorage groove__chunks__ManagedChunkStorage__new(groove_DynChunkKvStorage backend) {
    ManagedChunkStorage *m = (ManagedChunkStorage *)jz_rc_new(sizeof(ManagedChunkStorage), managed_drop);
    m->backend = backend;
    groove_DynChunkStorage d = {m, &groove__chunks__ManagedChunkStorage__ChunkStorage__VTBL};
    return d;
}

static jz_err managed_get(void *self, groove_Locator locator, groove_ContentHash expected_hash, jz_bytes *out) {
    ManagedChunkStorage *m = (ManagedChunkStorage *)self;
    bool found = false;
    groove_ContentHash hash;
    jz_bytes bytes = {0};
    JZ_TRY(m->backend.vt->get_exact(m->backend.self, locator, &found, &hash, &bytes));
    if (!found) return groove__chunks__ChunkStorageError__unavailable();
    /* Enforce the node ceiling before the integrity hash. */
    if (bytes.len > GROOVE_LV_MAX_ENCODED_NODE_BYTES || !hash_eq(&hash, &expected_hash)) {
        jz_bytes_drop(&bytes);
        return groove__chunks__ChunkStorageError__integrity();
    }
    groove_ContentHash actual = hash_of(bytes.ptr, bytes.len);
    if (!hash_eq(&actual, &expected_hash)) {
        jz_bytes_drop(&bytes);
        return groove__chunks__ChunkStorageError__integrity();
    }
    *out = bytes;
    return JZ_OK;
}

static jz_err managed_stage(void *self, groove_StagedChunkVec chunks, groove_StagedLargeValueAccounting *out) {
    ManagedChunkStorage *m = (ManagedChunkStorage *)self;
    /* Check every mechanically-verifiable property before the first put. */
    for (u32 i = 0; i < chunks.len; i++) {
        groove_StagedChunk *chunk = &chunks.ptr[i];
        if (chunk->encoded.len > GROOVE_LV_MAX_ENCODED_NODE_BYTES) {
            staged_chunks_drop(&chunks);
            return groove__chunks__ChunkStorageError__integrity();
        }
        groove_ContentHash h = hash_of(chunk->encoded.ptr, chunk->encoded.len);
        if (!hash_eq(&h, &chunk->node_ref.object_hash)) {
            staged_chunks_drop(&chunks);
            return groove__chunks__ChunkStorageError__integrity();
        }
    }
    groove_StagedLargeValueAccounting accounting = {0, 0};
    for (u32 i = 0; i < chunks.len; i++) {
        groove_StagedChunk *chunk = &chunks.ptr[i];
        u64 encoded_len = chunk->encoded.len;
        bool found = false;
        groove_ContentHash hash;
        jz_bytes bytes = {0};
        jz_bytes encoded = chunk->encoded;
        chunk->encoded = (jz_bytes){0};
        jz_err e = m->backend.vt->put_if_absent(m->backend.self, chunk->node_ref.locator, chunk->node_ref.object_hash, encoded, &found, &hash, &bytes);
        if (e.code) {
            staged_chunks_drop(&chunks);
            return e;
        }
        if (found) {
            bool conflict = bytes.len > GROOVE_LV_MAX_ENCODED_NODE_BYTES || !hash_eq(&hash, &chunk->node_ref.object_hash);
            if (!conflict) {
                groove_ContentHash h = hash_of(bytes.ptr, bytes.len);
                conflict = !hash_eq(&h, &chunk->node_ref.object_hash);
            }
            jz_bytes_drop(&bytes);
            if (conflict) {
                staged_chunks_drop(&chunks);
                return groove__chunks__ChunkStorageError__locator_conflict();
            }
        } else {
            accounting.encoded_bytes = accounting.encoded_bytes + encoded_len < accounting.encoded_bytes ? UINT64_MAX : accounting.encoded_bytes + encoded_len;
            accounting.node_count = accounting.node_count == UINT64_MAX ? UINT64_MAX : accounting.node_count + 1;
        }
    }
    staged_chunks_drop(&chunks);
    *out = accounting;
    return JZ_OK;
}

static jz_err managed_delete(void *self, groove_Locator locator, groove_ContentHash expected_hash) {
    ManagedChunkStorage *m = (ManagedChunkStorage *)self;
    return m->backend.vt->delete_exact(m->backend.self, locator, expected_hash);
}

const groove_ChunkStorageVtbl groove__chunks__ManagedChunkStorage__ChunkStorage__VTBL = {managed_get, managed_stage, managed_delete};

/* ======================================================================
 * OrderedChunkStorage
 * ====================================================================== */
typedef struct OrderedChunkStorage { void *storage; /* Weak<LayoutStorage> */ } OrderedChunkStorage;
static void ordered_drop(void *p) { rc_weak_release(((OrderedChunkStorage *)p)->storage); }

groove_DynChunkKvStorage groove__chunks__OrderedChunkStorage__new(const groove_LayoutStorage *storage) {
    OrderedChunkStorage *o = (OrderedChunkStorage *)jz_rc_new(sizeof(OrderedChunkStorage), ordered_drop);
    o->storage = rc_downgrade(storage);
    groove_DynChunkKvStorage d = {o, &groove__chunks__OrderedChunkStorage__ChunkKvStorage__VTBL};
    return d;
}

jz_bytes groove__chunks__OrderedChunkStorage__key(jz_slice locator) {
    jz_bytes key = jz_bytes_from((const u8 *)"chunk/", 6);
    jz_bytes_push(&key, locator.ptr, locator.len);
    return key;
}

static jz_bytes encode_with_install_receipt(groove_ContentHash hash, jz_slice bytes, const u8 receipt[16]) {
    jz_bytes value = {0};
    jz_vec_reserve(&value, 16 + 32 + bytes.len, 1);
    jz_bytes_push(&value, receipt, 16);
    jz_bytes_push(&value, hash.b, 32);
    jz_bytes_push(&value, bytes.ptr, bytes.len);
    return value;
}

/* consumes value */
jz_err groove__chunks__OrderedChunkStorage__decode(jz_bytes value, groove_ContentHash *hash, jz_bytes *bytes) {
    if (value.len < 16) {
        jz_bytes_drop(&value);
        return groove__chunks__ChunkStorageError__integrity();
    }
    const u8 *rest = value.ptr + 16;
    u32 rest_len = value.len - 16;
    if (rest_len < 32) {
        jz_bytes_drop(&value);
        return groove__chunks__ChunkStorageError__integrity();
    }
    const u8 *body = rest + 32;
    u32 body_len = rest_len - 32;
    /* Reject an oversized durable entry before copying or hashing it. */
    if (body_len > GROOVE_LV_MAX_ENCODED_NODE_BYTES) {
        jz_bytes_drop(&value);
        return groove__chunks__ChunkStorageError__integrity();
    }
    groove_ContentHash expected;
    memcpy(expected.b, rest, 32);
    groove_ContentHash actual = hash_of(body, body_len);
    if (!hash_eq(&actual, &expected)) {
        jz_bytes_drop(&value);
        return groove__chunks__ChunkStorageError__integrity();
    }
    *hash = expected;
    *bytes = jz_bytes_from(body, body_len);
    jz_bytes_drop(&value);
    return JZ_OK;
}

static jz_err ordered_storage(const OrderedChunkStorage *o, groove_LayoutStorage **out) {
    groove_LayoutStorage *s = (groove_LayoutStorage *)rc_upgrade(o->storage);
    if (!s) {
        static const char MSG[] = "owning Groove storage is closed";
        return groove__chunks__ChunkStorageError__backend(MSG, sizeof(MSG) - 1);
    }
    *out = s;
    return JZ_OK;
}

static jz_slice metadata_cf(void) {
    return jz_slice_of(groove__db__LARGE_VALUE_METADATA_CF, (u32)strlen(groove__db__LARGE_VALUE_METADATA_CF));
}

static jz_err ordered_get_exact(void *self, groove_Locator locator, bool *found, groove_ContentHash *hash, jz_bytes *bytes) {
    groove_LayoutStorage *storage;
    JZ_TRY(ordered_storage((OrderedChunkStorage *)self, &storage));
    groove_DynStorage dyn = groove__storage__LayoutStorage__as_dyn(storage);
    jz_bytes key = groove__chunks__OrderedChunkStorage__key(jz_slice_of(groove__large_values__Locator__as_bytes(&locator), sizeof(locator.b)));
    bool some = false;
    jz_bytes value = {0};
    jz_err e = groove__storage__OrderedKvStorage__get(&dyn, metadata_cf(), jz_bytes_slice(&key), &some, &value);
    jz_bytes_drop(&key);
    jz_rc_release(storage);
    if (e.code) return storage_backend_error(e);
    if (!some) {
        *found = false;
        return JZ_OK;
    }
    JZ_TRY(groove__chunks__OrderedChunkStorage__decode(value, hash, bytes));
    *found = true;
    return JZ_OK;
}

static jz_err ordered_put_if_absent(void *self, groove_Locator locator, groove_ContentHash hash, jz_bytes bytes, bool *found, groove_ContentHash *existing_hash, jz_bytes *existing) {
    groove_LayoutStorage *storage;
    jz_err e = ordered_storage((OrderedChunkStorage *)self, &storage);
    if (e.code) {
        jz_bytes_drop(&bytes);
        return e;
    }
    groove_DynStorage dyn = groove__storage__LayoutStorage__as_dyn(storage);
    jz_bytes key = groove__chunks__OrderedChunkStorage__key(jz_slice_of(groove__large_values__Locator__as_bytes(&locator), sizeof(locator.b)));
    jz_uuid receipt = jz_uuid_v4();
    jz_bytes candidate = encode_with_install_receipt(hash, jz_bytes_slice(&bytes), receipt.b);
    jz_bytes_drop(&bytes);
    bool some = false;
    jz_bytes prior = {0};
    e = groove__storage__OrderedKvStorage__put_if_absent(&dyn, metadata_cf(), jz_bytes_slice(&key), candidate, &some, &prior);
    jz_bytes_drop(&key);
    jz_rc_release(storage);
    if (e.code) return storage_backend_error(e);
    if (!some) {
        *found = false;
        return JZ_OK;
    }
    JZ_TRY(groove__chunks__OrderedChunkStorage__decode(prior, existing_hash, existing));
    *found = true;
    return JZ_OK;
}

static jz_err ordered_delete_exact(void *self, groove_Locator locator, groove_ContentHash expected_hash) {
    groove_LayoutStorage *storage;
    JZ_TRY(ordered_storage((OrderedChunkStorage *)self, &storage));
    groove_DynStorage dyn = groove__storage__LayoutStorage__as_dyn(storage);
    jz_bytes key = groove__chunks__OrderedChunkStorage__key(jz_slice_of(groove__large_values__Locator__as_bytes(&locator), sizeof(locator.b)));
    bool some = false;
    jz_bytes existing = {0};
    jz_err e = groove__storage__OrderedKvStorage__get(&dyn, metadata_cf(), jz_bytes_slice(&key), &some, &existing);
    if (e.code) {
        jz_bytes_drop(&key);
        jz_rc_release(storage);
        return storage_backend_error(e);
    }
    if (!some) {
        jz_bytes_drop(&key);
        jz_rc_release(storage);
        return JZ_OK;
    }
    groove_ContentHash hash;
    jz_bytes decoded;
    e = groove__chunks__OrderedChunkStorage__decode(jz_bytes_clone(&existing), &hash, &decoded);
    if (e.code) {
        jz_bytes_drop(&existing);
        jz_bytes_drop(&key);
        jz_rc_release(storage);
        return e;
    }
    jz_bytes_drop(&decoded);
    if (!hash_eq(&hash, &expected_hash)) {
        jz_bytes_drop(&existing);
        jz_bytes_drop(&key);
        jz_rc_release(storage);
        return groove__chunks__ChunkStorageError__integrity();
    }
    bool removed = false;
    e = groove__storage__OrderedKvStorage__compare_and_delete(&dyn, metadata_cf(), jz_bytes_slice(&key), jz_bytes_slice(&existing), &removed);
    jz_bytes_drop(&existing);
    jz_bytes_drop(&key);
    jz_rc_release(storage);
    if (e.code) return storage_backend_error(e);
    return JZ_OK;
}

const groove_ChunkKvStorageVtbl groove__chunks__OrderedChunkStorage__ChunkKvStorage__VTBL = {ordered_get_exact, ordered_put_if_absent, ordered_delete_exact};

/* ======================================================================
 * LocalChunkReader
 * ====================================================================== */
static void reader_cell_drop(void *p) { groove__chunks__DynChunkStorage__drop(&((groove_LocalChunkReaderCell *)p)->storage); }

groove_LocalChunkReader groove__chunks__LocalChunkReader__new(groove_DynChunkStorage storage) {
    groove_LocalChunkReaderCell *cell = (groove_LocalChunkReaderCell *)jz_rc_new(sizeof(groove_LocalChunkReaderCell), reader_cell_drop);
    cell->storage = storage;
    groove_LocalChunkReader r = {cell};
    return r;
}
groove_LocalChunkReader groove__chunks__LocalChunkReader__Clone__clone(const groove_LocalChunkReader *r) {
    groove_LocalChunkReader c = {(groove_LocalChunkReaderCell *)jz_rc_clone(r->storage)};
    return c;
}
void groove__chunks__LocalChunkReader__drop(groove_LocalChunkReader *r) {
    jz_rc_release(r->storage);
    r->storage = NULL;
}
void groove__chunks__LocalChunkReader__refresh_from(const groove_LocalChunkReader *r, const groove_LocalChunkReader *replacement) {
    groove_DynChunkStorage storage = groove__chunks__DynChunkStorage__clone(&replacement->storage->storage);
    groove_DynChunkStorage old = r->storage->storage;
    r->storage->storage = storage; /* RefCell::replace */
    groove__chunks__DynChunkStorage__drop(&old);
}
jz_err groove__chunks__LocalChunkReader__get(const groove_LocalChunkReader *r, groove_Locator locator, groove_ContentHash expected_hash, jz_bytes *out) {
    /* Do not retain the cell borrow across the backend operation. */
    groove_DynChunkStorage storage = groove__chunks__DynChunkStorage__clone(&r->storage->storage);
    jz_err e = groove__chunks__ChunkStorage__get(&storage, locator, expected_hash, out);
    groove__chunks__DynChunkStorage__drop(&storage);
    return e;
}

/* ======================================================================
 * Noop observer / journal, UnavailableChunkResolver
 * ====================================================================== */
typedef struct Unit { u8 unused; } Unit;
static jz_err noop_installed(void *self, groove_NodeRef node_ref, jz_bytes encoded) {
    jz_bytes_drop(&encoded);
    return JZ_OK;
}
static const groove_ChunkInstallObserverVtbl NOOP_OBSERVER_VTBL = {noop_installed, NULL};
groove_DynChunkInstallObserver groove__chunks__NoopChunkInstallObserver__new(void) {
    groove_DynChunkInstallObserver o = {jz_rc_new(sizeof(Unit), NULL), &NOOP_OBSERVER_VTBL};
    return o;
}
bool groove__chunks__ChunkInstallObserver__completes_install_journal(const groove_DynChunkInstallObserver *o) {
    return o->vt->completes_install_journal ? o->vt->completes_install_journal(o->self) : false;
}

static jz_err noop_mark_pending(void *self, groove_NodeRef node_ref) { return JZ_OK; }
static jz_err noop_is_pending(void *self, groove_NodeRef node_ref, bool *out) {
    *out = false;
    return JZ_OK;
}
static jz_err noop_complete(void *self, groove_NodeRef node_ref) { return JZ_OK; }
static const groove_ChunkInstallJournalVtbl NOOP_JOURNAL_VTBL = {noop_mark_pending, noop_is_pending, noop_complete};
groove_DynChunkInstallJournal groove__chunks__NoopChunkInstallJournal__new(void) {
    groove_DynChunkInstallJournal j = {jz_rc_new(sizeof(Unit), NULL), &NOOP_JOURNAL_VTBL};
    return j;
}

static jz_err unavailable_resolve(void *self, groove_ChunkRequest request, jz_bytes *out) { return groove__chunks__ChunkError__unavailable(); }
static const groove_MissingChunkResolverVtbl UNAVAILABLE_RESOLVER_VTBL = {unavailable_resolve};
groove_DynMissingChunkResolver groove__chunks__UnavailableChunkResolver__new(void) {
    groove_DynMissingChunkResolver r = {jz_rc_new(sizeof(Unit), NULL), &UNAVAILABLE_RESOLVER_VTBL};
    return r;
}

/* ======================================================================
 * ChunkProvider dispatch
 * ====================================================================== */
jz_err groove__chunks__ChunkProvider__get(const groove_DynChunkProvider *p, groove_ChunkRequest request, jz_bytes *out) {
    return p->vt->get(p->self, request, out);
}
bool groove__chunks__ChunkProvider__permits_eager_read_retry(const groove_DynChunkProvider *p) {
    return p->vt->permits_eager_read_retry ? p->vt->permits_eager_read_retry(p->self) : false;
}
jz_err groove__chunks__ChunkProvider__get_with_install_observer(const groove_DynChunkProvider *p, groove_ChunkRequest request, groove_DynChunkInstallObserver observer, jz_bytes *out) {
    if (p->vt->get_with_install_observer) return p->vt->get_with_install_observer(p->self, request, observer, out);
    jz_rc_release(observer.self);
    return groove__chunks__ChunkProvider__get(p, request, out);
}

/* ======================================================================
 * StorageChunkProvider<Rc<dyn ChunkStorage>>
 * ====================================================================== */
static const jz_btree_desc REQUEST_SET_DESC = {sizeof(groove_ChunkRequest), 0, groove__chunks__ChunkRequest__Ord__cmp, NULL, NULL};

typedef struct StorageChunkProvider {
    groove_DynChunkStorage storage;
    groove_DynMissingChunkResolver resolver;
    groove_DynChunkInstallObserver observer;
    groove_DynChunkInstallJournal journal;
    jz_btree *settled_installs; /* Rc<RefCell<BTreeSet<ChunkRequest>>> */
} StorageChunkProvider;

static void request_set_rc_drop(void *p) { jz_btree_drop((jz_btree *)p); }
static void storage_provider_drop(void *p) {
    StorageChunkProvider *s = (StorageChunkProvider *)p;
    jz_rc_release(s->storage.self);
    jz_rc_release(s->resolver.self);
    jz_rc_release(s->observer.self);
    jz_rc_release(s->journal.self);
    jz_rc_release(s->settled_installs);
}

static const groove_ChunkProviderVtbl STORAGE_PROVIDER_VTBL;

groove_DynChunkProvider groove__chunks__StorageChunkProvider__with_resolver_observer_and_journal(
    groove_DynChunkStorage storage, groove_DynMissingChunkResolver resolver,
    groove_DynChunkInstallObserver observer, groove_DynChunkInstallJournal journal) {
    StorageChunkProvider *s = (StorageChunkProvider *)jz_rc_new(sizeof(StorageChunkProvider), storage_provider_drop);
    s->storage = storage;
    s->resolver = resolver;
    s->observer = observer;
    s->journal = journal;
    s->settled_installs = (jz_btree *)jz_rc_new(sizeof(jz_btree), request_set_rc_drop);
    jz_btree_init(s->settled_installs, &REQUEST_SET_DESC);
    groove_DynChunkProvider d = {s, &STORAGE_PROVIDER_VTBL};
    return d;
}
groove_DynChunkProvider groove__chunks__StorageChunkProvider__with_resolver(groove_DynChunkStorage storage, groove_DynMissingChunkResolver resolver) {
    return groove__chunks__StorageChunkProvider__with_resolver_observer_and_journal(storage, resolver, groove__chunks__NoopChunkInstallObserver__new(),
                                                                                   groove__chunks__NoopChunkInstallJournal__new());
}
groove_DynChunkProvider groove__chunks__StorageChunkProvider__new(groove_DynChunkStorage storage) {
    return groove__chunks__StorageChunkProvider__with_resolver(storage, groove__chunks__UnavailableChunkResolver__new());
}

/* consumes bytes and observer */
static jz_err reconcile_pending_install(StorageChunkProvider *s, groove_ChunkRequest request, groove_NodeRef node_ref, jz_bytes bytes,
                                        groove_DynChunkInstallObserver observer, bool newly_staged) {
    jz_err e = JZ_OK;
    /* A miss which was re-staged must not be hidden by an earlier cache entry. */
    if (!newly_staged && jz_btree_contains(s->settled_installs, &request)) goto done;
    /* A regular resident chunk has no journal marker: no observer, no lifecycle mutex. */
    if (!newly_staged) {
        bool pending = false;
        e = s->journal.vt->is_pending(s->journal.self, node_ref, &pending);
        if (e.code) goto done;
        if (!pending) {
            jz_btree_insert(s->settled_installs, &request, NULL, NULL);
            goto done;
        }
    }
    e = observer.vt->installed(observer.self, node_ref, bytes);
    bytes = (jz_bytes){0};
    if (e.code) goto done;
    /* A crash before this compare-and-delete makes the next opener retry an
     * idempotent install; the recovery obligation is never lost. */
    if (!groove__chunks__ChunkInstallObserver__completes_install_journal(&observer)) {
        e = s->journal.vt->complete(s->journal.self, node_ref);
        if (e.code) goto done;
    }
    /* Do not cache a provisional (not yet durable) publication result. */
    {
        bool pending = false;
        e = s->journal.vt->is_pending(s->journal.self, node_ref, &pending);
        if (e.code) goto done;
        if (!pending) jz_btree_insert(s->settled_installs, &request, NULL, NULL);
    }
done:
    jz_bytes_drop(&bytes);
    jz_rc_release(observer.self);
    return e;
}

static jz_err storage_provider_get_with_install_observer(void *self, groove_ChunkRequest request, groove_DynChunkInstallObserver observer, jz_bytes *out) {
    StorageChunkProvider *s = (StorageChunkProvider *)self;
    groove_NodeRef node_ref;
    memcpy(node_ref.object_hash.b, request.object_hash, 32);
    node_ref.locator = request.locator;
    jz_bytes bytes = {0};
    jz_err e = groove__chunks__ChunkStorage__get(&s->storage, request.locator, node_ref.object_hash, &bytes);
    if (!e.code) {
        e = reconcile_pending_install(s, request, node_ref, jz_bytes_clone(&bytes), observer, false);
        if (e.code) {
            jz_bytes_drop(&bytes);
            return e;
        }
        *out = bytes;
        return JZ_OK;
    }
    if (e.code != GROOVE_CHUNK_STORAGE_E_Unavailable) {
        jz_rc_release(observer.self);
        return groove__chunks__ChunkError__From__from(e);
    }
    jz_err_drop(&e);
    e = s->resolver.vt->resolve(s->resolver.self, request, &bytes);
    if (e.code) {
        jz_rc_release(observer.self);
        return e;
    }
    /* Do not create a durable recovery obligation until the resolver supplied
     * mechanically valid bytes. */
    groove_ContentHash h = bytes.len > GROOVE_LV_MAX_ENCODED_NODE_BYTES ? (groove_ContentHash){{0}} : hash_of(bytes.ptr, bytes.len);
    if (bytes.len > GROOVE_LV_MAX_ENCODED_NODE_BYTES || !hash_eq(&h, &node_ref.object_hash)) {
        jz_bytes_drop(&bytes);
        jz_rc_release(observer.self);
        return groove__chunks__ChunkError__integrity();
    }
    /* The durable marker is written before the independent byte store. */
    e = s->journal.vt->mark_pending(s->journal.self, node_ref);
    if (e.code) {
        jz_bytes_drop(&bytes);
        jz_rc_release(observer.self);
        return e;
    }
    groove_StagedChunkVec staged = {0};
    groove_StagedChunk chunk = {node_ref, jz_bytes_clone(&bytes)};
    JZ_VEC_PUSH(staged, chunk);
    groove_StagedLargeValueAccounting accounting;
    e = groove__chunks__ChunkStorage__stage(&s->storage, staged, &accounting);
    if (e.code) {
        jz_bytes_drop(&bytes);
        jz_rc_release(observer.self);
        return groove__chunks__ChunkError__From__from(e);
    }
    e = reconcile_pending_install(s, request, node_ref, jz_bytes_clone(&bytes), observer, true);
    if (e.code) {
        jz_bytes_drop(&bytes);
        return e;
    }
    *out = bytes;
    return JZ_OK;
}

static jz_err storage_provider_get(void *self, groove_ChunkRequest request, jz_bytes *out) {
    StorageChunkProvider *s = (StorageChunkProvider *)self;
    groove_DynChunkInstallObserver observer = {jz_rc_clone(s->observer.self), s->observer.vt};
    return storage_provider_get_with_install_observer(self, request, observer, out);
}

static const groove_ChunkProviderVtbl STORAGE_PROVIDER_VTBL = {storage_provider_get, NULL, storage_provider_get_with_install_observer};

/* ======================================================================
 * PublicationInstallFailures
 * ====================================================================== */
static void err_drop_v(void *p) { jz_err_drop((jz_err *)p); }
static const jz_btree_desc FAILURES_DESC = {sizeof(groove_ChunkRequest), sizeof(jz_err), groove__chunks__ChunkRequest__Ord__cmp, NULL, err_drop_v};

groove_PublicationInstallFailures groove__chunks__PublicationInstallFailures__Default__default(void) {
    groove_PublicationInstallFailures f;
    f.failures = (jz_btree *)jz_rc_new(sizeof(jz_btree), request_set_rc_drop);
    jz_btree_init(f.failures, &FAILURES_DESC);
    return f;
}
groove_PublicationInstallFailures groove__chunks__PublicationInstallFailures__Clone__clone(const groove_PublicationInstallFailures *f) {
    groove_PublicationInstallFailures c = {(jz_btree *)jz_rc_clone(f->failures)};
    return c;
}
void groove__chunks__PublicationInstallFailures__drop(groove_PublicationInstallFailures *f) {
    jz_rc_release(f->failures);
    f->failures = NULL;
}
void groove__chunks__PublicationInstallFailures__record(const groove_PublicationInstallFailures *f, groove_NodeRef node_ref, jz_err error) {
    groove_ChunkRequest request;
    memcpy(request.object_hash, node_ref.object_hash.b, 32);
    request.locator = node_ref.locator;
    jz_err *slot = (jz_err *)jz_btree_get(f->failures, &request);
    if (slot) {
        jz_err_drop(slot);
        *slot = error;
    } else {
        jz_btree_insert(f->failures, &request, &error, NULL);
    }
}
static bool publication_install_failures_take(const groove_PublicationInstallFailures *f, const groove_ChunkRequest *request, jz_err *out) {
    groove_ChunkRequest k;
    return jz_btree_remove(f->failures, request, &k, out);
}

/* ======================================================================
 * ChunkLease
 * ====================================================================== */
static groove_ChunkLease *chunk_lease_new(jz_bytes bytes, const u8 verified_object_hash[32], groove_ChunkLeaseStats *stats) {
    stats->active += 1;
    stats->bytes = stats->bytes + bytes.len < stats->bytes ? SIZE_MAX : stats->bytes + bytes.len;
    groove_ChunkLease *l = JZ_NEW(groove_ChunkLease);
    l->bytes = bytes;
    memcpy(l->verified_object_hash, verified_object_hash, 32);
    l->stats = (groove_ChunkLeaseStats *)jz_rc_clone(stats);
    return l;
}
const jz_bytes *groove__chunks__ChunkLease__bytes(const groove_ChunkLease *l) { return &l->bytes; }
bool groove__chunks__ChunkLease__verifies_object_hash(const groove_ChunkLease *l, const u8 expected[32]) {
    return memcmp(l->verified_object_hash, expected, 32) == 0;
}
bool groove__chunks__ChunkLease__PartialEq__eq(const groove_ChunkLease *a, const groove_ChunkLease *b) { return jz_bytes_eq(&a->bytes, &b->bytes); }
void groove__chunks__ChunkLease__Drop__drop(groove_ChunkLease *l) {
    if (!l) return;
    groove_ChunkLeaseStats *counters = l->stats;
    counters->active = counters->active ? counters->active - 1 : 0;
    counters->bytes = counters->bytes >= l->bytes.len ? counters->bytes - l->bytes.len : 0;
    jz_rc_release(l->stats);
    jz_bytes_drop(&l->bytes);
    jz_free(l);
}

/* ======================================================================
 * OwnedChunkProvider state
 * ====================================================================== */
typedef struct CacheEntry { jz_bytes bytes; u64 last_use; } CacheEntry;
static void cache_entry_drop(void *p) { jz_bytes_drop(&((CacheEntry *)p)->bytes); }
static const jz_btree_desc CACHE_DESC = {sizeof(groove_ChunkRequest), sizeof(CacheEntry), groove__chunks__ChunkRequest__Ord__cmp, NULL, cache_entry_drop};

typedef struct VerifiedChunkCache { jz_btree entries; usize bytes; u64 clock; usize budget; } VerifiedChunkCache;
static void cache_rc_drop(void *p) { jz_btree_drop(&((VerifiedChunkCache *)p)->entries); }

typedef struct ChunkActivityState { usize active_requests; bool reclaiming; JZ_VEC(jz_waker) waiters; } ChunkActivityState;
static void activity_rc_drop(void *p) { jz_vec_drop(&((ChunkActivityState *)p)->waiters, sizeof(jz_waker), NULL); }

/* ChunkWake { waiters: Mutex<Vec<ChunkWaiter>> } */
typedef struct ChunkWaiter { u64 consumer_id; jz_waker waker; } ChunkWaiter;
typedef JZ_VEC(ChunkWaiter) ChunkWaiterVec;
typedef struct ChunkWake { ChunkWaiterVec waiters; } ChunkWake;
static void chunk_wake_rc_drop(void *p) { jz_vec_drop(&((ChunkWake *)p)->waiters, sizeof(ChunkWaiter), NULL); }

/* ArcWake::wake_by_ref: wake every consumer, never while holding the registry. */
static void groove__chunks__ChunkWake__ArcWake__wake_by_ref(ChunkWake *this_) {
    JZ_VEC(jz_waker) wakers = {0};
    for (u32 i = 0; i < this_->waiters.len; i++) JZ_VEC_PUSH(wakers, this_->waiters.ptr[i].waker);
    for (u32 i = 0; i < wakers.len; i++) jz_waker_wake(&wakers.ptr[i]);
    jz_vec_drop(&wakers, sizeof(jz_waker), NULL);
}

/* load_and_verify_chunk's captured state (the boxed provider future) */
typedef struct LoadFuture {
    groove_DynChunkProvider provider;
    VerifiedChunkCache *cache;
    groove_ChunkRequest request;
    bool has_install_observer;
    groove_DynChunkInstallObserver install_observer;
    bool has_install_failures;
    groove_PublicationInstallFailures install_failures;
} LoadFuture;
static void load_future_drop(LoadFuture *f) {
    jz_rc_release(f->provider.self);
    jz_rc_release(f->cache);
    if (f->has_install_observer) jz_rc_release(f->install_observer.self);
    if (f->has_install_failures) groove__chunks__PublicationInstallFailures__drop(&f->install_failures);
    jz_free(f);
}

typedef struct InFlightChunk {
    LoadFuture *future; /* NULL only while a consumer polls it */
    bool has_result;
    bool result_ok;
    jz_bytes result_bytes;
    groove_OwnedChunkError result_error;
    ChunkWake *wake; /* Arc<ChunkWake> */
    usize consumers;
    u64 next_consumer_id;
} InFlightChunk;
static void in_flight_chunk_drop(void *p) {
    InFlightChunk *c = (InFlightChunk *)p;
    if (c->future) load_future_drop(c->future);
    jz_bytes_drop(&c->result_bytes);
    jz_err_drop(&c->result_error.error);
    jz_rc_release(c->wake);
}
static const jz_btree_desc IN_FLIGHT_DESC = {sizeof(groove_ChunkRequest), sizeof(InFlightChunk), groove__chunks__ChunkRequest__Ord__cmp, NULL, in_flight_chunk_drop};
typedef struct InFlightChunks { jz_btree entries; } InFlightChunks;
static void in_flight_rc_drop(void *p) { jz_btree_drop(&((InFlightChunks *)p)->entries); }

struct groove_OwnedChunkProviderState {
    groove_DynChunkProvider provider;
    VerifiedChunkCache *cache;
    groove_ChunkLeaseStats *leases;
    ChunkActivityState *activity;
    InFlightChunks *in_flight;
    bool has_install_observer;
    groove_DynChunkInstallObserver install_observer;
    bool has_install_failures;
    groove_PublicationInstallFailures install_failures;
};
static void owned_state_drop(void *p) {
    groove_OwnedChunkProviderState *s = (groove_OwnedChunkProviderState *)p;
    jz_rc_release(s->provider.self);
    jz_rc_release(s->cache);
    jz_rc_release(s->leases);
    jz_rc_release(s->activity);
    jz_rc_release(s->in_flight);
    if (s->has_install_observer) jz_rc_release(s->install_observer.self);
    if (s->has_install_failures) groove__chunks__PublicationInstallFailures__drop(&s->install_failures);
}

static InFlightChunks *in_flight_new(void) {
    InFlightChunks *f = (InFlightChunks *)jz_rc_new(sizeof(InFlightChunks), in_flight_rc_drop);
    jz_btree_init(&f->entries, &IN_FLIGHT_DESC);
    return f;
}

groove_OwnedChunkProvider groove__chunks__OwnedChunkProvider__new_with_budget(groove_DynChunkProvider provider, usize budget_bytes) {
    groove_OwnedChunkProviderState *s = (groove_OwnedChunkProviderState *)jz_rc_new(sizeof(groove_OwnedChunkProviderState), owned_state_drop);
    s->provider = provider;
    s->cache = (VerifiedChunkCache *)jz_rc_new(sizeof(VerifiedChunkCache), cache_rc_drop);
    jz_btree_init(&s->cache->entries, &CACHE_DESC);
    s->cache->bytes = 0;
    s->cache->clock = 0;
    s->cache->budget = budget_bytes;
    s->leases = (groove_ChunkLeaseStats *)jz_rc_new(sizeof(groove_ChunkLeaseStats), NULL);
    s->activity = (ChunkActivityState *)jz_rc_new(sizeof(ChunkActivityState), activity_rc_drop);
    s->in_flight = in_flight_new();
    s->has_install_observer = false;
    s->has_install_failures = false;
    groove_OwnedChunkProvider p = {s};
    return p;
}
groove_OwnedChunkProvider groove__chunks__OwnedChunkProvider__new(groove_DynChunkProvider provider) {
    return groove__chunks__OwnedChunkProvider__new_with_budget(provider, 16u * 1024u * 1024u);
}
groove_OwnedChunkProvider groove__chunks__OwnedChunkProvider__Clone__clone(const groove_OwnedChunkProvider *p) {
    groove_OwnedChunkProvider c = {(groove_OwnedChunkProviderState *)jz_rc_clone(p->inner)};
    return c;
}
void groove__chunks__OwnedChunkProvider__drop(groove_OwnedChunkProvider *p) {
    jz_rc_release(p->inner);
    p->inner = NULL;
}

groove_OwnedChunkProvider groove__chunks__OwnedChunkProvider__with_install_observer(const groove_OwnedChunkProvider *p, groove_DynChunkInstallObserver observer, groove_PublicationInstallFailures failures) {
    const groove_OwnedChunkProviderState *src = p->inner;
    groove_OwnedChunkProviderState *s = (groove_OwnedChunkProviderState *)jz_rc_new(sizeof(groove_OwnedChunkProviderState), owned_state_drop);
    s->provider.self = jz_rc_clone(src->provider.self);
    s->provider.vt = src->provider.vt;
    s->cache = (VerifiedChunkCache *)jz_rc_clone(src->cache);
    s->leases = (groove_ChunkLeaseStats *)jz_rc_clone(src->leases);
    s->activity = (ChunkActivityState *)jz_rc_clone(src->activity);
    s->in_flight = in_flight_new();
    s->has_install_observer = true;
    s->install_observer = observer;
    s->has_install_failures = true;
    s->install_failures = failures;
    groove_OwnedChunkProvider out = {s};
    return out;
}

groove_ChunkCacheStats groove__chunks__OwnedChunkProvider__cache_stats(const groove_OwnedChunkProvider *p) {
    const groove_OwnedChunkProviderState *s = p->inner;
    groove_ChunkCacheStats st = {s->cache->entries.len, s->cache->bytes, s->cache->budget, s->leases->active, s->leases->bytes, s->activity->active_requests};
    return st;
}

bool groove__chunks__OwnedChunkProvider__permits_eager_read_retry(const groove_OwnedChunkProvider *p) {
    return groove__chunks__ChunkProvider__permits_eager_read_retry(&p->inner->provider);
}

/* ---- ChunkReclamationGuard ---- */
struct groove_ChunkReclamationGuard { ChunkActivityState *activity; };

static void wake_all(JZ_VEC(jz_waker) *waiters) {
    for (u32 i = 0; i < waiters->len; i++) jz_waker_wake(&waiters->ptr[i]);
    jz_vec_drop(waiters, sizeof(jz_waker), NULL);
}

groove_ChunkReclamationGuard *groove__chunks__OwnedChunkProvider__try_begin_reclamation(const groove_OwnedChunkProvider *p) {
    groove_OwnedChunkProviderState *s = p->inner;
    if (s->leases->active != 0) return NULL;
    ChunkActivityState *state = s->activity;
    if (state->reclaiming || state->active_requests != 0) return NULL;
    state->reclaiming = true;
    groove_ChunkReclamationGuard *g = JZ_NEW(groove_ChunkReclamationGuard);
    g->activity = (ChunkActivityState *)jz_rc_clone(s->activity);
    return g;
}
void groove__chunks__ChunkReclamationGuard__drop(groove_ChunkReclamationGuard *guard) {
    if (!guard) return;
    ChunkActivityState *state = guard->activity;
    state->reclaiming = false;
    JZ_VEC(jz_waker) waiters = {state->waiters.ptr, state->waiters.len, state->waiters.cap}; /* mem::take */
    state->waiters.ptr = NULL;
    state->waiters.len = state->waiters.cap = 0;
    wake_all((void *)&waiters);
    jz_rc_release(guard->activity);
    jz_free(guard);
}

/* ---- ActiveChunkRequest ---- */
typedef struct ActiveChunkRequest { ChunkActivityState *activity; } ActiveChunkRequest;

static jz_err active_chunk_request_acquire(ChunkActivityState *activity, const jz_waker *waker, ActiveChunkRequest *out) {
    if (activity->reclaiming) {
        if (waker) {
            bool registered = false;
            for (u32 i = 0; i < activity->waiters.len; i++)
                if (activity->waiters.ptr[i].wake == waker->wake && activity->waiters.ptr[i].ctx == waker->ctx) registered = true;
            if (!registered) JZ_VEC_PUSH(activity->waiters, *waker);
        }
        /* Poll::Pending in Rust: the request cannot be admitted this turn. */
        return groove__chunks__ChunkError__reentrant();
    }
    activity->active_requests = activity->active_requests == SIZE_MAX ? SIZE_MAX : activity->active_requests + 1;
    out->activity = (ChunkActivityState *)jz_rc_clone(activity);
    return JZ_OK;
}
static void active_chunk_request_drop(ActiveChunkRequest *r) {
    ChunkActivityState *state = r->activity;
    state->active_requests = state->active_requests ? state->active_requests - 1 : 0;
    if (state->active_requests == 0) {
        JZ_VEC(jz_waker) waiters = {state->waiters.ptr, state->waiters.len, state->waiters.cap};
        state->waiters.ptr = NULL;
        state->waiters.len = state->waiters.cap = 0;
        wake_all((void *)&waiters);
    }
    jz_rc_release(r->activity);
    r->activity = NULL;
}

/* ---- load_and_verify_chunk (polled once: completes synchronously) ---- */
static bool load_and_verify_chunk(LoadFuture *f, jz_bytes *out, groove_OwnedChunkError *err) {
    jz_bytes bytes = {0};
    if (f->has_install_observer) {
        groove_DynChunkInstallObserver observer = {jz_rc_clone(f->install_observer.self), f->install_observer.vt};
        jz_err result = groove__chunks__ChunkProvider__get_with_install_observer(&f->provider, f->request, observer, &bytes);
        jz_err failure;
        if (f->has_install_failures && publication_install_failures_take(&f->install_failures, &f->request, &failure)) {
            jz_err_drop(&result);
            jz_bytes_drop(&bytes);
            err->error = failure;
            err->publication_metadata_durability = true;
            return false;
        }
        if (result.code) {
            err->error = result;
            err->publication_metadata_durability = false;
            return false;
        }
    } else {
        jz_err result = groove__chunks__ChunkProvider__get(&f->provider, f->request, &bytes);
        if (result.code) {
            err->error = result; /* OwnedChunkError::from */
            err->publication_metadata_durability = false;
            return false;
        }
    }
    bool bad = bytes.len > GROOVE_LV_MAX_ENCODED_NODE_BYTES;
    if (!bad) {
        groove_ContentHash h = hash_of(bytes.ptr, bytes.len);
        bad = memcmp(h.b, f->request.object_hash, 32) != 0;
    }
    if (bad) {
        jz_bytes_drop(&bytes);
        err->error = groove__chunks__ChunkError__integrity();
        err->publication_metadata_durability = false;
        return false;
    }
    VerifiedChunkCache *cache = f->cache;
    usize length = bytes.len;
    if (length <= cache->budget) {
        while ((cache->bytes + length < cache->bytes ? SIZE_MAX : cache->bytes + length) > cache->budget) {
            /* min_by_key(last_use) */
            groove_ChunkRequest *k, *oldest = NULL;
            CacheEntry *v;
            u64 best = 0;
            JZ_BTREE_FOR(&cache->entries, k, v) {
                if (!oldest || v->last_use < best) {
                    oldest = k;
                    best = v->last_use;
                }
            }
            if (!oldest) break;
            groove_ChunkRequest oldest_key = *oldest, removed_key;
            CacheEntry evicted;
            if (jz_btree_remove(&cache->entries, &oldest_key, &removed_key, &evicted)) {
                cache->bytes = cache->bytes >= evicted.bytes.len ? cache->bytes - evicted.bytes.len : 0;
                cache_entry_drop(&evicted);
            }
        }
        cache->clock += 1; /* wrapping_add */
        CacheEntry entry = {jz_bytes_clone(&bytes), cache->clock};
        CacheEntry old;
        if (jz_btree_insert(&cache->entries, &f->request, &entry, &old)) cache_entry_drop(&old);
        cache->bytes = cache->bytes + length < cache->bytes ? SIZE_MAX : cache->bytes + length;
    }
    *out = bytes;
    return true;
}

/* ---- CoalescedChunkGet ---- */
typedef struct CoalescedChunkGet {
    groove_ChunkRequest request;
    u64 consumer_id;
    InFlightChunks *in_flight;
    groove_ChunkLeaseStats *leases;
    bool done;
} CoalescedChunkGet;

static void groove__chunks__CoalescedChunkGet__finish(CoalescedChunkGet *g) {
    if (g->done) return;
    g->done = true;
    InFlightChunk *entry = (InFlightChunk *)jz_btree_get(&g->in_flight->entries, &g->request);
    if (!entry) return;
    entry->consumers = entry->consumers ? entry->consumers - 1 : 0;
    /* retain(|waiter| waiter.consumer_id != self.consumer_id) */
    u32 w = 0;
    for (u32 i = 0; i < entry->wake->waiters.len; i++)
        if (entry->wake->waiters.ptr[i].consumer_id != g->consumer_id) entry->wake->waiters.ptr[w++] = entry->wake->waiters.ptr[i];
    entry->wake->waiters.len = w;
    /* Prompt surviving consumers to re-check after cancellation. */
    ChunkWaiterVec wake = {0};
    if (entry->consumers != 0 && !entry->has_result) {
        wake = entry->wake->waiters;
        entry->wake->waiters = (ChunkWaiterVec){0};
    }
    bool remove = entry->consumers == 0;
    if (remove) {
        groove_ChunkRequest k;
        InFlightChunk v;
        if (jz_btree_remove(&g->in_flight->entries, &g->request, &k, &v)) in_flight_chunk_drop(&v);
    }
    for (u32 i = 0; i < wake.len; i++) jz_waker_wake(&wake.ptr[i].waker);
    jz_vec_drop(&wake, sizeof(ChunkWaiter), NULL);
}

static void coalesced_drop(CoalescedChunkGet *g) {
    groove__chunks__CoalescedChunkGet__finish(g);
    jz_rc_release(g->in_flight);
    jz_rc_release(g->leases);
}

/* Future::poll, completing synchronously */
static jz_err coalesced_poll(CoalescedChunkGet *g, const jz_waker *waker, groove_ChunkLease **out, bool *publication_metadata_durability) {
    InFlightChunk *entry = (InFlightChunk *)jz_btree_get(&g->in_flight->entries, &g->request);
    JZ_ASSERT(entry != NULL, "coalesced chunk request remains registered while a consumer exists");
    bool ok;
    jz_bytes bytes = {0};
    groove_OwnedChunkError err = {JZ_OK, false};
    if (entry->has_result) {
        ok = entry->result_ok;
        if (ok) bytes = jz_bytes_clone(&entry->result_bytes);
        else {
            err.error = groove__chunks__ChunkError__Clone__clone(&entry->result_error.error);
            err.publication_metadata_durability = entry->result_error.publication_metadata_durability;
        }
    } else {
        if (waker) {
            bool found = false;
            for (u32 i = 0; i < entry->wake->waiters.len; i++) {
                ChunkWaiter *wt = &entry->wake->waiters.ptr[i];
                if (wt->consumer_id == g->consumer_id) {
                    wt->waker = *waker; /* replace unless will_wake */
                    found = true;
                }
            }
            if (!found) {
                ChunkWaiter wt = {g->consumer_id, *waker};
                JZ_VEC_PUSH(entry->wake->waiters, wt);
            }
        }
        LoadFuture *future = entry->future;
        if (!future) {
            ok = false;
            err.error = groove__chunks__ChunkError__reentrant();
        } else {
            entry->future = NULL; /* take() while polling outside the registry */
            ChunkWake *wake_bridge = (ChunkWake *)jz_rc_clone(entry->wake);
            ok = load_and_verify_chunk(future, &bytes, &err);
            load_future_drop(future);
            entry = (InFlightChunk *)jz_btree_get(&g->in_flight->entries, &g->request);
            JZ_ASSERT(entry != NULL, "coalesced chunk request remains registered while a consumer exists");
            entry->has_result = true;
            entry->result_ok = ok;
            if (ok) entry->result_bytes = jz_bytes_clone(&bytes);
            else {
                entry->result_error.error = groove__chunks__ChunkError__Clone__clone(&err.error);
                entry->result_error.publication_metadata_durability = err.publication_metadata_durability;
            }
            /* wake every other consumer */
            ChunkWaiterVec wake = entry->wake->waiters;
            entry->wake->waiters = (ChunkWaiterVec){0};
            for (u32 i = 0; i < wake.len; i++)
                if (wake.ptr[i].consumer_id != g->consumer_id) jz_waker_wake(&wake.ptr[i].waker);
            jz_vec_drop(&wake, sizeof(ChunkWaiter), NULL);
            jz_rc_release(wake_bridge);
        }
    }
    groove__chunks__CoalescedChunkGet__finish(g);
    if (!ok) {
        if (publication_metadata_durability) *publication_metadata_durability = err.publication_metadata_durability;
        return err.error;
    }
    *out = chunk_lease_new(bytes, g->request.object_hash, g->leases);
    return JZ_OK;
}

jz_err groove__chunks__OwnedChunkProvider__get_tracked(const groove_OwnedChunkProvider *p, groove_ChunkRequest request, const jz_waker *waker, groove_ChunkLease **out, bool *publication_metadata_durability) {
    groove_OwnedChunkProviderState *s = p->inner;
    if (publication_metadata_durability) *publication_metadata_durability = false;
    /* Admission must precede even a verified-cache hit. */
    ActiveChunkRequest request_guard;
    JZ_TRY(active_chunk_request_acquire(s->activity, waker, &request_guard));
    VerifiedChunkCache *cache = s->cache;
    cache->clock += 1;
    CacheEntry *cached = (CacheEntry *)jz_btree_get(&cache->entries, &request);
    if (cached) {
        cached->last_use = cache->clock;
        *out = chunk_lease_new(jz_bytes_clone(&cached->bytes), request.object_hash, s->leases);
        active_chunk_request_drop(&request_guard);
        return JZ_OK;
    }
    u64 consumer_id;
    InFlightChunk *entry = (InFlightChunk *)jz_btree_get(&s->in_flight->entries, &request);
    if (entry) {
        entry->consumers = entry->consumers == SIZE_MAX ? SIZE_MAX : entry->consumers + 1;
        consumer_id = entry->next_consumer_id;
        entry->next_consumer_id += 1;
    } else {
        LoadFuture *f = JZ_NEW(LoadFuture);
        f->provider.self = jz_rc_clone(s->provider.self);
        f->provider.vt = s->provider.vt;
        f->cache = (VerifiedChunkCache *)jz_rc_clone(s->cache);
        f->request = request;
        f->has_install_observer = s->has_install_observer;
        if (s->has_install_observer) {
            f->install_observer.self = jz_rc_clone(s->install_observer.self);
            f->install_observer.vt = s->install_observer.vt;
        }
        f->has_install_failures = s->has_install_failures;
        if (s->has_install_failures) f->install_failures = groove__chunks__PublicationInstallFailures__Clone__clone(&s->install_failures);
        InFlightChunk fresh = {0};
        fresh.future = f;
        fresh.wake = (ChunkWake *)jz_rc_new(sizeof(ChunkWake), chunk_wake_rc_drop);
        fresh.consumers = 1;
        fresh.next_consumer_id = 1;
        jz_btree_insert(&s->in_flight->entries, &request, &fresh, NULL);
        consumer_id = 0;
    }
    CoalescedChunkGet get = {request, consumer_id, (InFlightChunks *)jz_rc_clone(s->in_flight), (groove_ChunkLeaseStats *)jz_rc_clone(s->leases), false};
    jz_err e = coalesced_poll(&get, waker, out, publication_metadata_durability);
    coalesced_drop(&get);
    active_chunk_request_drop(&request_guard);
    return e;
}

jz_err groove__chunks__OwnedChunkProvider__get(const groove_OwnedChunkProvider *p, groove_ChunkRequest request, groove_ChunkLease **out) {
    /* future.await.map_err(Into::into): OwnedChunkError -> ChunkError */
    return groove__chunks__OwnedChunkProvider__get_tracked(p, request, NULL, out, NULL);
}

/* ---- UnavailableChunkProvider / Default ---- */
static jz_err unavailable_provider_get(void *self, groove_ChunkRequest request, jz_bytes *out) { return groove__chunks__ChunkError__unavailable(); }
static const groove_ChunkProviderVtbl UNAVAILABLE_PROVIDER_VTBL = {unavailable_provider_get, NULL, NULL};
groove_OwnedChunkProvider groove__chunks__OwnedChunkProvider__Default__default(void) {
    groove_DynChunkProvider p = {jz_rc_new(sizeof(Unit), NULL), &UNAVAILABLE_PROVIDER_VTBL};
    return groove__chunks__OwnedChunkProvider__new(p);
}
