/* groove::chunks (crates/groove/src/chunks.rs)
 *
 * async -> sync. Every `Rc<dyn Trait>` handle is {self, vt} where `self` is a
 * jz_rc payload (clone = jz_rc_clone, drop = jz_rc_release; the payload's
 * destructor was given to jz_rc_new). */
#ifndef GROOVE_CHUNKS_H
#define GROOVE_CHUNKS_H
#include "jz_rt.h"
#include "large_values.h"
#include "storage/mod.h"

/* ChunkStorageError */
enum {
    GROOVE_CHUNK_STORAGE_E_BASE = 0x1200,
    GROOVE_CHUNK_STORAGE_E_Unavailable,     /* "chunk is unavailable" */
    GROOVE_CHUNK_STORAGE_E_LocatorConflict, /* "opaque locator already names different content" */
    GROOVE_CHUNK_STORAGE_E_Integrity,       /* "staged chunk failed integrity validation" */
    GROOVE_CHUNK_STORAGE_E_Backend,         /* "chunk storage failed: {0}" (msg) */
    GROOVE_CHUNK_STORAGE_E_END
};
/* ChunkError */
enum {
    GROOVE_CHUNK_E_BASE = 0x1210,
    GROOVE_CHUNK_E_Unavailable, /* "chunk is unavailable" */
    GROOVE_CHUNK_E_Retryable,   /* "chunk retrieval is retryable after {retry_after_ms}ms" */
    GROOVE_CHUNK_E_Backend,     /* "chunk retrieval failed: {0}" */
    GROOVE_CHUNK_E_Integrity,   /* "chunk bytes do not match the requested object hash" */
    GROOVE_CHUNK_E_Reentrant,   /* "chunk request re-entered while its backing request was being polled" */
    GROOVE_CHUNK_E_END
};
jz_err groove__chunks__ChunkStorageError__unavailable(void);
jz_err groove__chunks__ChunkStorageError__locator_conflict(void);
jz_err groove__chunks__ChunkStorageError__integrity(void);
jz_err groove__chunks__ChunkStorageError__backend(const char *p, u32 n);
jz_err groove__chunks__ChunkError__unavailable(void);
jz_err groove__chunks__ChunkError__retryable(u32 retry_after_ms);
jz_err groove__chunks__ChunkError__backend(const char *p, u32 n);
jz_err groove__chunks__ChunkError__integrity(void);
jz_err groove__chunks__ChunkError__reentrant(void);
/* impl From<ChunkStorageError> for ChunkError (consumes e) */
jz_err groove__chunks__ChunkError__From__from(jz_err e);
u32 groove__chunks__ChunkError__retry_after_ms(const jz_err *e);
jz_err groove__chunks__ChunkError__Clone__clone(const jz_err *e);

/* ChunkRequest { object_hash, locator } */
typedef struct groove_ChunkRequest { u8 object_hash[32]; groove_Locator locator; } groove_ChunkRequest;
typedef JZ_VEC(groove_ChunkRequest) groove_ChunkRequestVec;
int groove__chunks__ChunkRequest__Ord__cmp(const void *a, const void *b, void *ctx);
bool groove__chunks__ChunkRequest__PartialEq__eq(const groove_ChunkRequest *a, const groove_ChunkRequest *b);

/* ---- trait ChunkKvStorage ---- */
typedef struct groove_ChunkKvStorageVtbl {
    /* Option<(ContentHash, Bytes)> -> *found */
    jz_err (*get_exact)(void *self, groove_Locator locator, bool *found, groove_ContentHash *hash, jz_bytes *bytes);
    jz_err (*put_if_absent)(void *self, groove_Locator locator, groove_ContentHash hash, jz_bytes bytes, bool *found, groove_ContentHash *existing_hash, jz_bytes *existing);
    jz_err (*delete_exact)(void *self, groove_Locator locator, groove_ContentHash expected_hash);
} groove_ChunkKvStorageVtbl;
typedef struct groove_DynChunkKvStorage { void *self; const groove_ChunkKvStorageVtbl *vt; } groove_DynChunkKvStorage;

/* ---- trait ChunkStorage ---- */
typedef struct groove_ChunkStorageVtbl {
    jz_err (*get)(void *self, groove_Locator locator, groove_ContentHash expected_hash, jz_bytes *out);
    jz_err (*stage)(void *self, groove_StagedChunkVec chunks, groove_StagedLargeValueAccounting *out);
    jz_err (*delete_)(void *self, groove_Locator locator, groove_ContentHash expected_hash); /* NULL = trait default */
} groove_ChunkStorageVtbl;
typedef struct groove_DynChunkStorage { void *self; const groove_ChunkStorageVtbl *vt; } groove_DynChunkStorage;
jz_err groove__chunks__ChunkStorage__get(const groove_DynChunkStorage *s, groove_Locator locator, groove_ContentHash expected_hash, jz_bytes *out);
jz_err groove__chunks__ChunkStorage__stage(const groove_DynChunkStorage *s, groove_StagedChunkVec chunks, groove_StagedLargeValueAccounting *out);
jz_err groove__chunks__ChunkStorage__delete(const groove_DynChunkStorage *s, groove_Locator locator, groove_ContentHash expected_hash);
groove_DynChunkStorage groove__chunks__DynChunkStorage__clone(const groove_DynChunkStorage *s);
void groove__chunks__DynChunkStorage__drop(groove_DynChunkStorage *s);

/* ManagedChunkStorage { backend: Rc<dyn ChunkKvStorage> } */
groove_DynChunkStorage groove__chunks__ManagedChunkStorage__new(groove_DynChunkKvStorage backend);
extern const groove_ChunkStorageVtbl groove__chunks__ManagedChunkStorage__ChunkStorage__VTBL;

/* OrderedChunkStorage { storage: Weak<LayoutStorage> } (storage is a jz_rc payload) */
groove_DynChunkKvStorage groove__chunks__OrderedChunkStorage__new(const groove_LayoutStorage *storage);
extern const groove_ChunkKvStorageVtbl groove__chunks__OrderedChunkStorage__ChunkKvStorage__VTBL;
jz_bytes groove__chunks__OrderedChunkStorage__key(jz_slice locator);
jz_err groove__chunks__OrderedChunkStorage__decode(jz_bytes value, groove_ContentHash *hash, jz_bytes *bytes);

/* ---- LocalChunkReader { storage: Rc<RefCell<Rc<dyn ChunkStorage>>> } ---- */
typedef struct groove_LocalChunkReaderCell { groove_DynChunkStorage storage; } groove_LocalChunkReaderCell;
typedef struct groove_LocalChunkReader { groove_LocalChunkReaderCell *storage; /* jz_rc payload */ } groove_LocalChunkReader;
groove_LocalChunkReader groove__chunks__LocalChunkReader__new(groove_DynChunkStorage storage);
groove_LocalChunkReader groove__chunks__LocalChunkReader__Clone__clone(const groove_LocalChunkReader *r);
void groove__chunks__LocalChunkReader__drop(groove_LocalChunkReader *r);
void groove__chunks__LocalChunkReader__refresh_from(const groove_LocalChunkReader *r, const groove_LocalChunkReader *replacement);
jz_err groove__chunks__LocalChunkReader__get(const groove_LocalChunkReader *r, groove_Locator locator, groove_ContentHash expected_hash, jz_bytes *out);

/* ---- trait MissingChunkResolver ---- */
typedef struct groove_MissingChunkResolverVtbl {
    jz_err (*resolve)(void *self, groove_ChunkRequest request, jz_bytes *out);
} groove_MissingChunkResolverVtbl;
typedef struct groove_DynMissingChunkResolver { void *self; const groove_MissingChunkResolverVtbl *vt; } groove_DynMissingChunkResolver;
groove_DynMissingChunkResolver groove__chunks__UnavailableChunkResolver__new(void);

/* ---- trait ChunkInstallObserver ---- */
typedef struct groove_ChunkInstallObserverVtbl {
    jz_err (*installed)(void *self, groove_NodeRef node_ref, jz_bytes encoded);
    bool (*completes_install_journal)(void *self); /* NULL = trait default false */
} groove_ChunkInstallObserverVtbl;
typedef struct groove_DynChunkInstallObserver { void *self; const groove_ChunkInstallObserverVtbl *vt; } groove_DynChunkInstallObserver;
bool groove__chunks__ChunkInstallObserver__completes_install_journal(const groove_DynChunkInstallObserver *o);
groove_DynChunkInstallObserver groove__chunks__NoopChunkInstallObserver__new(void);

/* ---- trait ChunkInstallJournal ---- */
typedef struct groove_ChunkInstallJournalVtbl {
    jz_err (*mark_pending)(void *self, groove_NodeRef node_ref);
    jz_err (*is_pending)(void *self, groove_NodeRef node_ref, bool *out);
    jz_err (*complete)(void *self, groove_NodeRef node_ref);
} groove_ChunkInstallJournalVtbl;
typedef struct groove_DynChunkInstallJournal { void *self; const groove_ChunkInstallJournalVtbl *vt; } groove_DynChunkInstallJournal;
groove_DynChunkInstallJournal groove__chunks__NoopChunkInstallJournal__new(void);

/* ---- trait ChunkProvider ---- */
typedef struct groove_ChunkProviderVtbl {
    jz_err (*get)(void *self, groove_ChunkRequest request, jz_bytes *out);
    bool (*permits_eager_read_retry)(void *self); /* NULL = default false */
    /* observer is an owned Rc handle; NULL = default (ignores observer) */
    jz_err (*get_with_install_observer)(void *self, groove_ChunkRequest request, groove_DynChunkInstallObserver observer, jz_bytes *out);
} groove_ChunkProviderVtbl;
typedef struct groove_DynChunkProvider { void *self; const groove_ChunkProviderVtbl *vt; } groove_DynChunkProvider;
jz_err groove__chunks__ChunkProvider__get(const groove_DynChunkProvider *p, groove_ChunkRequest request, jz_bytes *out);
bool groove__chunks__ChunkProvider__permits_eager_read_retry(const groove_DynChunkProvider *p);
jz_err groove__chunks__ChunkProvider__get_with_install_observer(const groove_DynChunkProvider *p, groove_ChunkRequest request, groove_DynChunkInstallObserver observer, jz_bytes *out);

/* StorageChunkProvider<Rc<dyn ChunkStorage>> -> Rc<dyn ChunkProvider> (consumes handles) */
groove_DynChunkProvider groove__chunks__StorageChunkProvider__new(groove_DynChunkStorage storage);
groove_DynChunkProvider groove__chunks__StorageChunkProvider__with_resolver(groove_DynChunkStorage storage, groove_DynMissingChunkResolver resolver);
groove_DynChunkProvider groove__chunks__StorageChunkProvider__with_resolver_observer_and_journal(
    groove_DynChunkStorage storage, groove_DynMissingChunkResolver resolver,
    groove_DynChunkInstallObserver observer, groove_DynChunkInstallJournal journal);

/* PublicationInstallFailures { failures: Rc<RefCell<BTreeMap<ChunkRequest, ChunkError>>> } */
typedef struct groove_PublicationInstallFailures { jz_btree *failures; /* jz_rc payload */ } groove_PublicationInstallFailures;
groove_PublicationInstallFailures groove__chunks__PublicationInstallFailures__Default__default(void);
groove_PublicationInstallFailures groove__chunks__PublicationInstallFailures__Clone__clone(const groove_PublicationInstallFailures *f);
void groove__chunks__PublicationInstallFailures__drop(groove_PublicationInstallFailures *f);
void groove__chunks__PublicationInstallFailures__record(const groove_PublicationInstallFailures *f, groove_NodeRef node_ref, jz_err error);

/* OwnedChunkError { error: ChunkError, publication_metadata_durability } */
typedef struct groove_OwnedChunkError { jz_err error; bool publication_metadata_durability; } groove_OwnedChunkError;

/* ChunkLease (heap) */
typedef struct groove_ChunkLeaseStats { usize active; usize bytes; } groove_ChunkLeaseStats;
typedef struct groove_ChunkLease { jz_bytes bytes; u8 verified_object_hash[32]; groove_ChunkLeaseStats *stats; /* jz_rc */ } groove_ChunkLease;
const jz_bytes *groove__chunks__ChunkLease__bytes(const groove_ChunkLease *l);
bool groove__chunks__ChunkLease__verifies_object_hash(const groove_ChunkLease *l, const u8 expected[32]);
bool groove__chunks__ChunkLease__PartialEq__eq(const groove_ChunkLease *a, const groove_ChunkLease *b);
void groove__chunks__ChunkLease__Drop__drop(groove_ChunkLease *l); /* frees l */

typedef struct groove_ChunkCacheStats {
    usize entries, owned_bytes, budget_bytes, active_leases, leased_bytes, active_requests;
} groove_ChunkCacheStats;

/* OwnedChunkProvider (Clone): inner is a jz_rc payload of the shared state */
typedef struct groove_OwnedChunkProviderState groove_OwnedChunkProviderState;
typedef struct groove_OwnedChunkProvider { groove_OwnedChunkProviderState *inner; } groove_OwnedChunkProvider;
groove_OwnedChunkProvider groove__chunks__OwnedChunkProvider__new(groove_DynChunkProvider provider);
groove_OwnedChunkProvider groove__chunks__OwnedChunkProvider__new_with_budget(groove_DynChunkProvider provider, usize budget_bytes);
groove_OwnedChunkProvider groove__chunks__OwnedChunkProvider__Default__default(void);
groove_OwnedChunkProvider groove__chunks__OwnedChunkProvider__Clone__clone(const groove_OwnedChunkProvider *p);
void groove__chunks__OwnedChunkProvider__drop(groove_OwnedChunkProvider *p);
groove_OwnedChunkProvider groove__chunks__OwnedChunkProvider__with_install_observer(const groove_OwnedChunkProvider *p, groove_DynChunkInstallObserver observer, groove_PublicationInstallFailures failures);
groove_ChunkCacheStats groove__chunks__OwnedChunkProvider__cache_stats(const groove_OwnedChunkProvider *p);
bool groove__chunks__OwnedChunkProvider__permits_eager_read_retry(const groove_OwnedChunkProvider *p);
/* Option<ChunkReclamationGuard>: NULL = None */
typedef struct groove_ChunkReclamationGuard groove_ChunkReclamationGuard;
groove_ChunkReclamationGuard *groove__chunks__OwnedChunkProvider__try_begin_reclamation(const groove_OwnedChunkProvider *p);
void groove__chunks__ChunkReclamationGuard__drop(groove_ChunkReclamationGuard *guard);
/* waker: the polling task's waker (may be NULL in the synchronous port) */
jz_err groove__chunks__OwnedChunkProvider__get(const groove_OwnedChunkProvider *p, groove_ChunkRequest request, groove_ChunkLease **out);
jz_err groove__chunks__OwnedChunkProvider__get_tracked(const groove_OwnedChunkProvider *p, groove_ChunkRequest request, const jz_waker *waker, groove_ChunkLease **out, bool *publication_metadata_durability);

#endif
