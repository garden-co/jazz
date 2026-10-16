/* groove::storage (crates/groove/src/storage/mod.rs, memory.rs, idb.rs, key_codec.rs)
 *
 * async -> sync. Trait objects are {self, vtbl}. Logical ownership follows
 * the Rust signatures: `cf: String` / `key: Vec<u8>` arguments are passed as
 * borrowed slices (callee copies what it keeps); value bodies (`Vec<u8>`) and
 * operation vectors are passed by value (callee owns them). */
#ifndef GROOVE_STORAGE_MOD_H
#define GROOVE_STORAGE_MOD_H
#include "jz_rt.h"
#include "../records/mod.h"

/* ---------------- Error ---------------- */
enum {
    GROOVE_STORAGE_E_BASE = 0x1300,
    GROOVE_STORAGE_E_ColumnFamilyNotFound,             /* "column family not found: {0}" */
    GROOVE_STORAGE_E_InvalidStorageLayout,             /* "invalid storage layout: {0}" */
    GROOVE_STORAGE_E_UnsupportedStorageCodecs,         /* "unsupported storage format: ..." */
    GROOVE_STORAGE_E_InvalidStorageKey,                /* "invalid storage key: {0}" */
    GROOVE_STORAGE_E_ConditionalMutationInTransaction, /* "conditional mutations require a direct storage boundary" */
    GROOVE_STORAGE_E_IdbGenerationContention,          /* "IndexedDB storage remained contended after {retries} generation-conflict retries" */
    GROOVE_STORAGE_E_Record,                           /* "record error: {0}" */
    GROOVE_STORAGE_E_Backend,                          /* "{backend} storage error: {message}" */
    GROOVE_STORAGE_E_END
    /* Error::IdbTree is #[error(transparent)]: idb_tree codes pass through. */
};
jz_err groove__storage__Error__column_family_not_found(jz_slice cf);
jz_err groove__storage__Error__invalid_storage_layout(const char *msg);
jz_err groove__storage__Error__invalid_storage_key(const char *msg);
jz_err groove__storage__Error__conditional_mutation_in_transaction(void);
jz_err groove__storage__Error__idb_generation_contention(usize retries);
jz_err groove__storage__Error__backend(const char *backend, jz_slice message);
jz_err groove__storage__Error__unsupported_storage_codecs(u16 epoch, const jz_str *missing, u32 nmissing, const jz_str *unknown, u32 nunknown);
/* impl From<records::Error> for Error (consumes e) */
jz_err groove__storage__Error__From_records__from(jz_err e);
bool groove__storage__Error__is_column_family_not_found(const jz_err *e);

/* ---------------- names ---------------- */
#define GROOVE_MAX_APPLICATION_STORAGE_NAME_BYTES 65535u
bool groove__storage__prefix_successor(jz_slice prefix, jz_bytes *out);
jz_err groove__storage__validate_physical_storage_name(jz_slice name);
jz_err groove__storage__validate_physical_storage_names(const jz_str *names, u32 n);
jz_err groove__storage__validate_physical_storage_names_cstr(const char *const *names, u32 n);
jz_err groove__storage__validate_application_storage_name(jz_slice name);

/* ---------------- scans ---------------- */
enum { GROOVE_SCAN_PREFIX = 0, GROOVE_SCAN_RANGE = 1 };
typedef struct groove_ScanBounds {
    u32 tag;
    jz_bytes start; /* Prefix(prefix) uses start */
    jz_bytes end;   /* Range only */
} groove_ScanBounds;
void groove__storage__ScanBounds__drop(groove_ScanBounds *b);
void groove__storage__ScanBounds__Clone__clone(groove_ScanBounds *dst, const groove_ScanBounds *src);
bool groove__storage__ScanBounds__is_empty_range(const groove_ScanBounds *b);

typedef enum { GROOVE_SCAN_FORWARD = 0, GROOVE_SCAN_REVERSE = 1 } groove_ScanDirection;

typedef struct groove_ScanRequest {
    jz_str cf;
    groove_ScanBounds bounds;
    groove_ScanDirection direction;
    bool has_max_items;
    usize max_items;
} groove_ScanRequest;
groove_ScanRequest groove__storage__ScanRequest__prefix(jz_str cf, jz_bytes prefix);
groove_ScanRequest groove__storage__ScanRequest__range(jz_str cf, jz_bytes start, jz_bytes end);
groove_ScanRequest groove__storage__ScanRequest__with_max_items(groove_ScanRequest r, usize max_items);
groove_ScanRequest groove__storage__ScanRequest__reversed(groove_ScanRequest r);
void groove__storage__ScanRequest__Clone__clone(groove_ScanRequest *dst, const groove_ScanRequest *src);
void groove__storage__ScanRequest__drop(groove_ScanRequest *r);

typedef struct groove_KeyValue { jz_bytes key; jz_bytes value; } groove_KeyValue;
typedef JZ_VEC(groove_KeyValue) groove_KeyValueVec;
void groove__storage__KeyValue__drop(void *p);
void groove__storage__KeyValue__clone(void *dst, const void *src);
void groove__storage__KeyValueVec__drop(groove_KeyValueVec *v);

/* ---------------- write operations ---------------- */
enum { GROOVE_WRITE_SET = 0, GROOVE_WRITE_DELETE = 1 };
typedef struct groove_OwnedWriteOperation { u32 tag; jz_str cf; jz_bytes key; jz_bytes value; } groove_OwnedWriteOperation;
typedef JZ_VEC(groove_OwnedWriteOperation) groove_OwnedWriteOperationVec;
void groove__storage__OwnedWriteOperation__drop(void *p);
void groove__storage__OwnedWriteOperation__clone(void *dst, const void *src);
void groove__storage__OwnedWriteOperationVec__drop(groove_OwnedWriteOperationVec *v);
bool groove__storage__OwnedWriteOperation__PartialEq__eq(const groove_OwnedWriteOperation *a, const groove_OwnedWriteOperation *b);

typedef struct groove_WriteOperation { u32 tag; jz_slice cf; jz_slice key; jz_slice value; } groove_WriteOperation;
typedef JZ_VEC(groove_WriteOperation) groove_WriteOperationVec;
groove_WriteOperation groove__storage__WriteOperation__set(jz_slice cf, jz_slice key, jz_slice value);
groove_WriteOperation groove__storage__WriteOperation__delete(jz_slice cf, jz_slice key);
groove_WriteOperation groove__storage__OwnedWriteOperation__as_write_operation(const groove_OwnedWriteOperation *op);
/* impl From<WriteOperation> for OwnedWriteOperation */
groove_OwnedWriteOperation groove__storage__OwnedWriteOperation__From__from(const groove_WriteOperation *op);

enum { GROOVE_WMO_COMMITTED = 0, GROOVE_WMO_UNCOMMITTED = 1, GROOVE_WMO_POSSIBLY_COMMITTED = 2 };
typedef struct groove_WriteManyOutcome { u32 tag; jz_err error; } groove_WriteManyOutcome;
bool groove__storage__WriteManyOutcome__is_committed(const groove_WriteManyOutcome *o);
const jz_err *groove__storage__WriteManyOutcome__error(const groove_WriteManyOutcome *o);
bool groove__storage__WriteManyOutcome__may_have_committed(const groove_WriteManyOutcome *o);

typedef enum { GROOVE_VALUE_ABSENT = 0, GROOVE_VALUE_IDENTICAL = 1, GROOVE_VALUE_DIFFERENT = 2 } groove_ValueComparison;

/* ---------------- StorageCursor ---------------- */
typedef struct groove_StorageCursorVtbl {
    /* Ok(None) -> *some = false */
    jz_err (*next_batch)(void *self, bool *some, groove_KeyValueVec *out);
    void (*drop)(void *self);
} groove_StorageCursorVtbl;
/* StorageScan<'a> = Box<dyn StorageCursor + 'a> */
typedef struct groove_StorageScan { void *self; const groove_StorageCursorVtbl *vt; } groove_StorageScan;
jz_err groove__storage__StorageCursor__next_batch(groove_StorageScan *scan, bool *some, groove_KeyValueVec *out);
void groove__storage__StorageScan__drop(groove_StorageScan *scan);
groove_StorageScan groove__storage__ReadyStorageCursor__new(groove_KeyValueVec values);
/* consumes scan */
jz_err groove__storage__collect_scan(groove_StorageScan scan, groove_KeyValueVec *out);

/* ---------------- OrderedKvStorage ---------------- */
/* groove_StrVec (Vec<String>) comes from records/values.h */
void groove__storage__StrVec__drop(groove_StrVec *v);

typedef struct groove_DynStorage groove_DynStorage;
typedef struct groove_OrderedKvStorageVtbl {
    bool (*permits_eager_read_retry)(void *self);                                                       /* NULL -> false */
    jz_err (*get)(void *self, jz_slice cf, jz_slice key, bool *some, jz_bytes *out);
    jz_err (*compare_value)(void *self, jz_slice cf, jz_slice key, jz_slice expected, groove_ValueComparison *out); /* NULL -> default */
    jz_err (*put_if_absent)(void *self, jz_slice cf, jz_slice key, jz_bytes value, bool *some, jz_bytes *existing);
    jz_err (*compare_and_delete)(void *self, jz_slice cf, jz_slice key, jz_slice expected, bool *removed);
    jz_err (*set)(void *self, jz_slice cf, jz_slice key, jz_bytes value);
    jz_err (*delete_)(void *self, jz_slice cf, jz_slice key);
    jz_err (*close)(void *self);                                                                        /* NULL -> Ok */
    jz_err (*set_write_flush_cadence)(void *self, usize every);                                         /* NULL -> Ok */
    jz_err (*flush_write_boundary)(void *self);                                                         /* NULL -> Ok */
    jz_err (*approximate_class_bytes)(void *self, jz_slice cf, bool *some, u64 *out);                   /* NULL -> Ok(None) */
    jz_err (*scan)(void *self, groove_ScanRequest request, groove_StorageScan *out);
    jz_err (*last_with_prefix)(void *self, jz_slice cf, jz_slice prefix, bool *some, groove_KeyValue *out); /* NULL -> default */
    jz_err (*last_with_prefix_before_or_at)(void *self, jz_slice cf, jz_slice prefix, jz_slice upper, bool *some, groove_KeyValue *out); /* NULL -> default */
    jz_err (*write_many)(void *self, groove_OwnedWriteOperationVec operations);
    groove_WriteManyOutcome (*write_many_outcome)(void *self, groove_OwnedWriteOperationVec operations); /* NULL -> default */
    groove_WriteManyOutcome (*write_many_borrowed_outcome)(void *self, groove_WriteOperationVec operations); /* NULL -> default */
    bool (*column_family_names)(void *self, groove_StrVec *out);                                        /* NULL -> None */
    jz_err (*range)(void *self, jz_slice cf, jz_slice start, jz_slice end, groove_KeyValueVec *out);    /* NULL -> default */
    jz_err (*prefix)(void *self, jz_slice cf, jz_slice prefix, groove_KeyValueVec *out);                /* NULL -> default */
    /* ErasedReopenableStorage::reopen_boxed; NULL when the type is not reopenable. Consumes self. */
    jz_err (*reopen)(void *self, groove_StrVec column_families, groove_DynStorage *out);
    void (*drop)(void *self);
} groove_OrderedKvStorageVtbl;
/* &dyn OrderedKvStorage / Box<dyn ErasedReopenableStorage> / Rc<dyn ..> */
struct groove_DynStorage { void *self; const groove_OrderedKvStorageVtbl *vt; };

bool groove__storage__OrderedKvStorage__permits_eager_read_retry(const groove_DynStorage *s);
jz_err groove__storage__OrderedKvStorage__get(const groove_DynStorage *s, jz_slice cf, jz_slice key, bool *some, jz_bytes *out);
jz_err groove__storage__OrderedKvStorage__compare_value(const groove_DynStorage *s, jz_slice cf, jz_slice key, jz_slice expected, groove_ValueComparison *out);
jz_err groove__storage__OrderedKvStorage__put_if_absent(const groove_DynStorage *s, jz_slice cf, jz_slice key, jz_bytes value, bool *some, jz_bytes *existing);
jz_err groove__storage__OrderedKvStorage__compare_and_delete(const groove_DynStorage *s, jz_slice cf, jz_slice key, jz_slice expected, bool *removed);
jz_err groove__storage__OrderedKvStorage__set(const groove_DynStorage *s, jz_slice cf, jz_slice key, jz_bytes value);
jz_err groove__storage__OrderedKvStorage__delete(const groove_DynStorage *s, jz_slice cf, jz_slice key);
jz_err groove__storage__OrderedKvStorage__close(const groove_DynStorage *s);
jz_err groove__storage__OrderedKvStorage__set_write_flush_cadence(const groove_DynStorage *s, usize every);
jz_err groove__storage__OrderedKvStorage__flush_write_boundary(const groove_DynStorage *s);
usize groove__storage__OrderedKvStorage__cache_token(const groove_DynStorage *s);
jz_err groove__storage__OrderedKvStorage__approximate_class_bytes(const groove_DynStorage *s, jz_slice cf, bool *some, u64 *out);
jz_err groove__storage__OrderedKvStorage__scan(const groove_DynStorage *s, groove_ScanRequest request, groove_StorageScan *out);
jz_err groove__storage__OrderedKvStorage__last_with_prefix(const groove_DynStorage *s, jz_slice cf, jz_slice prefix, bool *some, groove_KeyValue *out);
jz_err groove__storage__OrderedKvStorage__last_with_prefix_before_or_at(const groove_DynStorage *s, jz_slice cf, jz_slice prefix, jz_slice upper, bool *some, groove_KeyValue *out);
jz_err groove__storage__OrderedKvStorage__write_many(const groove_DynStorage *s, groove_OwnedWriteOperationVec operations);
groove_WriteManyOutcome groove__storage__OrderedKvStorage__write_many_outcome(const groove_DynStorage *s, groove_OwnedWriteOperationVec operations);
groove_WriteManyOutcome groove__storage__OrderedKvStorage__write_many_borrowed_outcome(const groove_DynStorage *s, groove_WriteOperationVec operations);
bool groove__storage__OrderedKvStorage__column_family_names(const groove_DynStorage *s, groove_StrVec *out);
jz_err groove__storage__OrderedKvStorage__range(const groove_DynStorage *s, jz_slice cf, jz_slice start, jz_slice end, groove_KeyValueVec *out);
jz_err groove__storage__OrderedKvStorage__prefix(const groove_DynStorage *s, jz_slice cf, jz_slice prefix, groove_KeyValueVec *out);

/* ---------------- BoxedStorage / ReopenableStorage ---------------- */
typedef struct groove_BoxedStorage { groove_DynStorage inner; } groove_BoxedStorage;
groove_BoxedStorage groove__storage__BoxedStorage__new(groove_DynStorage storage);
groove_DynStorage groove__storage__BoxedStorage__as_dyn(groove_BoxedStorage *b);
void groove__storage__BoxedStorage__drop(groove_BoxedStorage *b);
/* ReopenableStorage::reopen for BoxedStorage (consumes self) */
jz_err groove__storage__BoxedStorage__ReopenableStorage__reopen(groove_BoxedStorage self, groove_StrVec column_families, groove_BoxedStorage *out);
/* ErasedReopenableStorage::reopen_boxed (consumes the box) */
jz_err groove__storage__ErasedReopenableStorage__reopen_boxed(groove_DynStorage self, groove_StrVec column_families, groove_DynStorage *out);

/* ---------------- OwnedStorage ---------------- */
/* OwnedStorage(Rc<dyn OrderedKvStorage>): rc is a jz_rc payload holding a DynStorage */
typedef struct groove_OwnedStorage { groove_DynStorage *rc; } groove_OwnedStorage;
groove_OwnedStorage groove__storage__OwnedStorage__new(groove_DynStorage storage);
groove_OwnedStorage groove__storage__OwnedStorage__Clone__clone(const groove_OwnedStorage *s);
void groove__storage__OwnedStorage__drop(groove_OwnedStorage *s);
const groove_DynStorage *groove__storage__OwnedStorage__as_ref(const groove_OwnedStorage *s);
jz_err groove__storage__OwnedStorage__get(const groove_OwnedStorage *s, jz_slice cf, jz_slice key, bool *some, jz_bytes *out);
jz_err groove__storage__OwnedStorage__scan(const groove_OwnedStorage *s, groove_ScanRequest request, groove_KeyValueVec *out);

/* ---------------- StorageLayout / LayoutStorage ---------------- */
typedef enum { GROOVE_LAYOUT_IDENTITY = 0, GROOVE_LAYOUT_JAZZ_CLASS_V2 = 1 } groove_StorageLayout;
groove_StorageLayout groove__storage__StorageLayout__jazz_class_v2(void);
groove_StrVec groove__storage__StorageLayout__physical_column_families(groove_StorageLayout layout, const jz_slice *logical, u32 n);

typedef struct groove_LayoutStorage { groove_BoxedStorage inner; groove_StorageLayout layout; } groove_LayoutStorage;
/* Returns a jz_rc payload (the database holds Rc<LayoutStorage>; chunks hold Weak). */
jz_err groove__storage__LayoutStorage__new(groove_DynStorage inner, groove_StorageLayout layout, groove_LayoutStorage **out);
jz_err groove__storage__LayoutStorage__new_boxed(groove_BoxedStorage inner, groove_StorageLayout layout, groove_LayoutStorage **out);
jz_err groove__storage__LayoutStorage__approximate_class_bytes_for_logical_families(const groove_LayoutStorage *s, const jz_slice *logical, u32 n, bool *some, u64 *out);
groove_BoxedStorage groove__storage__LayoutStorage__into_inner(groove_LayoutStorage *s);
void groove__storage__LayoutStorage__drop(groove_LayoutStorage *s); /* payload destructor */
extern const groove_OrderedKvStorageVtbl groove__storage__LayoutStorage__OrderedKvStorage__VTBL;
groove_DynStorage groove__storage__LayoutStorage__as_dyn(const groove_LayoutStorage *s);

/* ---------------- RecordStore ---------------- */
typedef struct groove_RecordStore {
    const groove_DynStorage *storage;
    jz_slice column_family;
    const groove_RecordDescriptor *descriptor;
} groove_RecordStore;
groove_RecordStore groove__storage__RecordStore__new(const groove_DynStorage *storage, jz_slice column_family, const groove_RecordDescriptor *descriptor);
const groove_RecordDescriptor *groove__storage__RecordStore__descriptor(const groove_RecordStore *r);
jz_slice groove__storage__RecordStore__column_family(const groove_RecordStore *r);
jz_err groove__storage__RecordStore__get_raw(const groove_RecordStore *r, jz_slice key, bool *some, jz_bytes *out);
jz_err groove__storage__RecordStore__get(const groove_RecordStore *r, jz_slice key, bool *some, groove_Record *out);
jz_err groove__storage__RecordStore__range(const groove_RecordStore *r, jz_slice start, jz_slice end, groove_KeyValueVec *out);
jz_err groove__storage__RecordStore__prefix(const groove_RecordStore *r, jz_slice prefix, groove_KeyValueVec *out);
jz_err groove__storage__RecordStore__range_reverse(const groove_RecordStore *r, jz_slice start, jz_slice end, groove_KeyValueVec *out);
jz_err groove__storage__RecordStore__scan(const groove_RecordStore *r, groove_ScanBounds bounds, groove_StorageScan *out);
jz_err groove__storage__RecordStore__last_with_prefix(const groove_RecordStore *r, jz_slice prefix, bool *some, groove_KeyValue *out);
jz_err groove__storage__RecordStore__last_with_prefix_before_or_at(const groove_RecordStore *r, jz_slice prefix, jz_slice upper, bool *some, groove_KeyValue *out);
jz_err groove__storage__RecordStore__put_if_absent(const groove_RecordStore *r, jz_slice key, jz_slice record, bool *some, groove_Record *out);
groove_OwnedWriteOperation groove__storage__RecordStore__set(const groove_RecordStore *r, jz_slice key, jz_slice record);
groove_OwnedWriteOperation groove__storage__RecordStore__delete(const groove_RecordStore *r, jz_slice key);
jz_err groove__storage__RecordStore__write_many(const groove_RecordStore *r, groove_OwnedWriteOperationVec operations);

/* ---------------- StagedWriteState ---------------- */
/* Rc<Vec<OwnedWriteOperation>> block: jz_rc payload */
typedef struct groove_OpBlock { groove_OwnedWriteOperationVec ops; } groove_OpBlock;
typedef JZ_VEC(groove_OpBlock *) groove_OpBlockVec;
void groove__storage__OpBlockVec__drop(groove_OpBlockVec *v);
typedef struct groove_StagedWriteState {
    groove_OpBlockVec operations;
    JZ_VEC(usize) operation_ends;
    bool has_latest_by_cf_key;
    jz_btree latest_by_cf_key; /* BTreeMap<jz_str, BTreeMap<jz_bytes, usize>> */
    usize point_reads_without_index;
} groove_StagedWriteState;
void groove__storage__StagedWriteState__Default__default(groove_StagedWriteState *out);
void groove__storage__StagedWriteState__drop(groove_StagedWriteState *s);
void groove__storage__StagedWriteState__Clone__clone(groove_StagedWriteState *dst, const groove_StagedWriteState *src);
bool groove__storage__StagedWriteState__is_empty(const groove_StagedWriteState *s);
const groove_OwnedWriteOperation *groove__storage__StagedWriteState__operation(const groove_StagedWriteState *s, usize index);
void groove__storage__StagedWriteState__stage(groove_StagedWriteState *s, groove_OwnedWriteOperation operation);
void groove__storage__StagedWriteState__extend(groove_StagedWriteState *s, groove_OwnedWriteOperationVec operations);
groove_OpBlockVec groove__storage__StagedWriteState__snapshot(const groove_StagedWriteState *s);
void groove__storage__StagedWriteState__extend_shared(groove_StagedWriteState *s, groove_OpBlockVec snapshot);
groove_OwnedWriteOperationVec groove__storage__StagedWriteState__into_operations(groove_StagedWriteState *s);
bool groove__storage__StagedWriteState__latest_index(groove_StagedWriteState *s, jz_slice cf, jz_slice key, usize *out);
void groove__storage__StagedWriteState__ensure_key_index(groove_StagedWriteState *s);
bool groove__storage__StagedWriteState__contains_key(groove_StagedWriteState *s, jz_slice cf, jz_slice key);
void groove__storage__StagedWriteState__From__from(groove_OwnedWriteOperationVec operations, groove_StagedWriteState *out);
/* VecDeque<(Vec<u8>, Option<Value>)> */
typedef struct groove_StagedEntry { jz_bytes key; bool has_value; jz_bytes value; } groove_StagedEntry;
void groove__storage__StagedWriteState__scan_snapshot(groove_StagedWriteState *s, jz_slice cf, const groove_ScanBounds *bounds, jz_deque *out);

/* ---------------- StagedWriteOverlay / StorageTransaction ---------------- */
/* OverlayHandle: owned handles are jz_rc payloads (released on drop). */
typedef struct groove_StagedWriteOverlay {
    groove_DynStorage base;
    void *base_rc;             /* OverlayHandle::Owned(Rc<S>): jz_rc payload released on drop, else NULL */
    groove_StagedWriteState *staged_writes;
    bool staged_owned;         /* staged_writes is a jz_rc payload */
} groove_StagedWriteOverlay;
enum { GROOVE_STAGED_MISS = 0, GROOVE_STAGED_SET = 1, GROOVE_STAGED_DELETE = 2 };
typedef struct groove_StagedPointValue { u32 tag; jz_bytes value; } groove_StagedPointValue;
groove_StagedWriteOverlay groove__storage__StagedWriteOverlay__new(groove_DynStorage base, groove_StagedWriteState *staged_writes);
groove_StagedWriteOverlay groove__storage__StagedWriteOverlay__new_owned(groove_DynStorage base, void *base_rc, groove_StagedWriteState *staged_writes_rc);
void groove__storage__StagedWriteOverlay__drop(groove_StagedWriteOverlay *o);
groove_StagedPointValue groove__storage__StagedWriteOverlay__staged_point_value(const groove_StagedWriteOverlay *o, jz_slice cf, jz_slice key);
void groove__storage__StagedWriteOverlay__stage(const groove_StagedWriteOverlay *o, groove_OwnedWriteOperation operation);
void groove__storage__StagedWriteOverlay__drain_into(const groove_StagedWriteOverlay *o, groove_OwnedWriteOperationVec *target);
extern const groove_OrderedKvStorageVtbl groove__storage__StagedWriteOverlay__OrderedKvStorage__VTBL;
groove_DynStorage groove__storage__StagedWriteOverlay__as_dyn(groove_StagedWriteOverlay *o);

typedef struct groove_StorageTransaction { groove_DynStorage base; groove_StagedWriteState staged_writes; } groove_StorageTransaction;
void groove__storage__StorageTransaction__new(groove_DynStorage base, groove_StorageTransaction *out);
void groove__storage__OrderedKvStorage__begin_txn(groove_DynStorage base, groove_StorageTransaction *out);
jz_err groove__storage__StorageTransaction__commit(groove_StorageTransaction *t);
bool groove__storage__StorageTransaction__is_empty(const groove_StorageTransaction *t);
void groove__storage__StorageTransaction__stage_owned_operations(groove_StorageTransaction *t, groove_OwnedWriteOperationVec operations);
void groove__storage__StorageTransaction__drop(groove_StorageTransaction *t);
extern const groove_OrderedKvStorageVtbl groove__storage__StorageTransaction__OrderedKvStorage__VTBL;
groove_DynStorage groove__storage__StorageTransaction__as_dyn(groove_StorageTransaction *t);

/* ---------------- key_codec ---------------- */
jz_err groove__storage__key_codec__encode_column_family_key(jz_slice cf, jz_slice key, jz_bytes *out);
jz_err groove__storage__key_codec__decode_column_family_key(jz_slice encoded, jz_slice *cf, jz_slice *key);
bool groove__storage__key_codec__increment_bytes(jz_slice bytes, jz_bytes *out);
bool groove__storage__key_codec__prefix_upper_bound(jz_slice prefix, jz_bytes *out);

/* ---------------- memory ---------------- */
enum {
    GROOVE_MEMSNAP_E_BASE = 0x1320,
    GROOVE_MEMSNAP_E_Invalid,             /* "memory storage snapshot is invalid: {0}" */
    GROOVE_MEMSNAP_E_TooLarge,            /* "memory storage snapshot {field} exceeds v1 bounds" */
    GROOVE_MEMSNAP_E_UnsupportedVersion,  /* "unsupported memory storage snapshot version {found}; expected {expected}" */
    GROOVE_MEMSNAP_E_InvalidColumnFamily, /* "memory storage snapshot has an invalid physical column-family name: {0}" */
    GROOVE_MEMSNAP_E_END
};
/* MemoryStorage { inner: Arc<Mutex<ColumnFamilies>> }: inner is a jz_rc payload */
typedef struct groove_MemoryColumnFamilies { jz_btree families; /* BTreeMap<jz_str, jz_btree<jz_bytes,jz_bytes>> */ } groove_MemoryColumnFamilies;
typedef struct groove_MemoryStorage { groove_MemoryColumnFamilies *inner; } groove_MemoryStorage;
jz_err groove__storage__memory__MemoryStorage__new(const char *const *column_families, u32 n, groove_MemoryStorage *out);
groove_MemoryStorage groove__storage__memory__MemoryStorage__Clone__clone(const groove_MemoryStorage *s);
void groove__storage__memory__MemoryStorage__drop(groove_MemoryStorage *s);
jz_err groove__storage__memory__MemoryStorage__export_snapshot(const groove_MemoryStorage *s, jz_bytes *out);
jz_err groove__storage__memory__MemoryStorage__import_snapshot(const groove_MemoryStorage *s, jz_slice bytes);
jz_err groove__storage__memory__MemoryStorage__ReopenableStorage__reopen(groove_MemoryStorage self, groove_StrVec column_families, groove_MemoryStorage *out);
extern const groove_OrderedKvStorageVtbl groove__storage__memory__MemoryStorage__OrderedKvStorage__VTBL;
/* Box<MemoryStorage> as Box<dyn ErasedReopenableStorage> (consumes s) */
groove_DynStorage groove__storage__memory__MemoryStorage__into_dyn(groove_MemoryStorage s);

#endif
