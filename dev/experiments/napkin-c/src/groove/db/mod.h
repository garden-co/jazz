/* db/mod.h: shared declarations of crates/groove/src/db/ (mod.rs, batch.rs,
 * commit.rs, encoding.rs, facade.rs, primary_storage.rs, query.rs,
 * schema_admission.rs, storage_helpers.rs): the schema-aware Database facade
 * over records, storage and the IVM runtime. */
#ifndef GROOVE_DB_MOD_H
#define GROOVE_DB_MOD_H
#include "jz_rt.h"
#include "../_ext_groove-ivm-db.h"
#include "../ivm/graph.h"
#include "../ivm/template.h"

/* "%.*s" helpers */
#define GROOVE_DB_S(s) (int)(s).len, (const char *)(s).ptr
#define GROOVE_DB_E(e) (int)((e).msg ? (e).msg->len : 0u), ((e).msg ? (e).msg->ptr : "")

/* Reserved Groove-owned metadata plane for staged roots and persisted
 * reference accounting. It is never exposed as an application table. */
#define GROOVE_DB_LARGE_VALUE_METADATA_CF "__groove_large_values"
extern const char groove__db__LARGE_VALUE_METADATA_CF[];

/* ---------------- db::Error ---------------- */
enum groove_DbErrorCode {
    GROOVE_DB_ERR_BASE = 0x0B00,
    GROOVE_DB_ImmutableBatchConflict,
    GROOVE_DB_StaleImmutableBatch,
    GROOVE_DB_DatabasePoisoned,
    GROOVE_DB_UnsettledPublications,
    GROOVE_DB_PublicationNotFound,
    GROOVE_DB_SubscriptionEnded,
    /* SubscriptionFailed: transparent */
    GROOVE_DB_DuplicatePrimaryKey,
    GROOVE_DB_DuplicateTableVariant,
    GROOVE_DB_TableVariantTagOutOfRange,
    GROOVE_DB_DuplicateParameter,
    /* IvmRuntime: transparent */
    GROOVE_DB_InvalidPersistedIndex,
    GROOVE_DB_InvalidLargeValueMetadata,
    GROOVE_DB_PendingLargeValueUploadLimitExceeded,
    GROOVE_DB_IndexKeyArity,
    GROOVE_DB_IndexNotFound,
    GROOVE_DB_MissingParameter,
    GROOVE_DB_MissingPrimaryKey,
    GROOVE_DB_InvalidTableVariantField,
    GROOVE_DB_PrimaryKeyArity,
    GROOVE_DB_PrimaryKeyTypeMismatch,
    /* QueryPlanning, RecordEncoding: transparent */
    GROOVE_DB_DirectRecordStoreNotFound,
    GROOVE_DB_InvalidDirectRecordStoreKey,
    GROOVE_DB_InvalidApplicationStorageName,
    GROOVE_DB_DuplicateApplicationStorageName,
    /* Storage: transparent */
    GROOVE_DB_TableNotFound,
    GROOVE_DB_TableAlreadyExists,
    GROOVE_DB_TableFieldDefinitionMismatch,
    GROOVE_DB_TableIndexDefinitionMismatch,
    GROOVE_DB_TableIndexRegistrationWhilePublicationsResident,
    GROOVE_DB_TableIndexFieldNotFound,
    GROOVE_DB_SchemaVersionMissingPrimaryKey,
    GROOVE_DB_UnsupportedSchemaVariantTableFeature,
    GROOVE_DB_SchemaVersionDescriptorMismatch,
    GROOVE_DB_ReservedTableVariant,
    GROOVE_DB_CannotPromoteLiveTableToSchemaVariants,
    GROOVE_DB_UnknownTableVariant,
    GROOVE_DB_UnknownParameter,
    GROOVE_DB_ERR_END
};
jz_err groove__db__Error__unit(u32 code);
jz_err groove__db__Error__publication_not_found(u64 publication);
jz_err groove__db__Error__duplicate_primary_key(const char *table, u32 tn, const u8 *key, u32 kn);
/* one-String tuple variants (DuplicateParameter, InvalidPersistedIndex, ..., UnknownParameter) */
jz_err groove__db__Error__str(u32 code, const char *p, u32 n);
jz_err groove__db__Error__invalid_large_value_metadata(const char *fmt, ...);
jz_err groove__db__Error__invalid_persisted_index(const char *fmt, ...);
/* { table, version } variants */
jz_err groove__db__Error__table_version(u32 code, const char *table, u32 tn, u64 version);
jz_err groove__db__Error__pending_upload_limit(u32 limit);
/* { index|table, expected, actual } */
jz_err groove__db__Error__arity(u32 code, const char *name, u32 n, u32 expected, u32 actual);
/* { a, b } String pairs */
jz_err groove__db__Error__pair(u32 code, const char *a, u32 na, const char *b, u32 nb);
jz_err groove__db__Error__table_index_field_not_found(const char *table, u32 tn, const char *index, u32 in, const char *field, u32 fn);
/* { table, version, field|column } */
jz_err groove__db__Error__table_version_field(u32 code, const char *table, u32 tn, u64 version, const char *field, u32 fn);
void groove__db__Error__Debug__fmt(jz_str *out, const jz_err *error);
static inline bool groove__db__is_storage_error(u32 code) { return code > GROOVE_EXT_STORAGE_ERR_BASE && code < GROOVE_EXT_STORAGE_ERR_END; }

/* ---------------- large-value metadata plane (mod.rs) ---------------- */
typedef struct groove_LargeValueRootReferences { u64 durable; u64 staged; bool node_active; } groove_LargeValueRootReferences;
typedef struct groove_LargeValueNodeReferences { u64 references; u64 upload_references; groove_NodeRefVec children; } groove_LargeValueNodeReferences;
void groove__db__LargeValueNodeReferences__drop(groove_LargeValueNodeReferences *r);
enum { GROOVE_RECLAIM_CLEAR_STALE_QUEUE, GROOVE_RECLAIM_WAIT_FOR_DURABLE_ZERO, GROOVE_RECLAIM_RESIDENT_VETO, GROOVE_RECLAIM_DELETE };
typedef struct groove_CompletedLargeValueUpload {
    groove_StagedLargeValueId id;
    groove_StagedLargeValueId receipt_id;
    groove_LargeValueRef descriptor;
    groove_StagedLargeValueAccounting accounting;
} groove_CompletedLargeValueUpload;
void groove__db__CompletedLargeValueUpload__drop(groove_CompletedLargeValueUpload *u);
/* (NodeRef, i8) */
typedef struct groove_NodeTransition { groove_NodeRef node_ref; i8 delta; } groove_NodeTransition;
typedef JZ_VEC(groove_NodeTransition) groove_NodeTransitionVec;
/* BTreeMap<NodeRef, LargeValueNodeReferences> / BTreeSet<NodeRef> / BTreeMap<NodeRef, i64|u64> */
extern const jz_btree_desc groove__db__NODE_UPDATES_DESC;
extern const jz_btree_desc groove__db__NODE_SET_DESC;
extern const jz_btree_desc groove__db__NODE_I64_DESC;
extern const jz_btree_desc groove__db__NODE_U64_DESC;
int groove__db__node_ref_cmp(const void *a, const void *b, void *ctx);

jz_bytes groove__db__staged_large_value_key(groove_StagedLargeValueId id);
jz_bytes groove__db__pending_large_value_upload_key(groove_StagedLargeValueId id);
jz_bytes groove__db__completed_large_value_upload_key(groove_StagedLargeValueId id);
jz_bytes groove__db__completed_large_value_receipt_key(groove_StagedLargeValueId id);
jz_err groove__db__large_value_root_key(const groove_NodeRef *node_ref, jz_bytes *out);
jz_err groove__db__large_value_node_key(const groove_NodeRef *node_ref, jz_bytes *out);
jz_err groove__db__large_value_reclaim_key(const groove_NodeRef *node_ref, jz_bytes *out);
jz_err groove__db__large_value_reclaim_node_ref_from_key(const u8 *key, u32 n, groove_NodeRef *out);
jz_err groove__db__large_value_pending_install_key(const groove_NodeRef *node_ref, jz_bytes *out);
u8 groove__db__large_value_reclaim_decision(const groove_LargeValueNodeReferences *durable, const groove_LargeValueNodeReferences *resident);
groove_NodeRefVec groove__db__unique_large_value_children(const groove_ChunkNode *node);
groove_NodeRefVec groove__db__canonical_large_value_children(groove_NodeRefVec children);
jz_err groove__db__encode_completed_large_value_upload(const groove_CompletedLargeValueUpload *upload, jz_bytes *out);
jz_err groove__db__encode_large_value_root_references(const groove_LargeValueRootReferences *r, jz_bytes *out);
jz_err groove__db__decode_large_value_root_references(const u8 *p, u32 n, groove_LargeValueRootReferences *out);
jz_err groove__db__encode_large_value_node_references(const groove_LargeValueNodeReferences *r, jz_bytes *out);
jz_err groove__db__decode_large_value_node_references(const u8 *p, u32 n, groove_LargeValueNodeReferences *out);
jz_err groove__db__encode_staged_large_value(const groove_StagedLargeValue *staged, jz_bytes *out);
jz_err groove__db__decode_staged_large_value_at_key(const u8 *key, u32 kn, const u8 *p, u32 n, groove_StagedLargeValue *out);
jz_err groove__db__encode_pending_large_value_upload(const groove_PendingLargeValueUpload *upload, jz_bytes *out);
jz_err groove__db__decode_pending_large_value_upload_at_key(const u8 *key, u32 kn, const u8 *p, u32 n, groove_PendingLargeValueUpload *out);
jz_err groove__db__decode_completed_large_value_upload_at_key(const u8 *key, u32 kn, const u8 *p, u32 n, groove_CompletedLargeValueUpload *out);
jz_err groove__db__decode_completed_large_value_receipt_at_key(const u8 *key, u32 kn, const u8 *p, u32 n, groove_StagedLargeValueId *out);
/* consumes node_updates and pending */
jz_err groove__db__large_value_node_transition_operations(groove_DynStorage storage, jz_btree node_updates, groove_NodeTransitionVec pending,
                                                          bool allow_missing_positive_metadata, groove_WriteOpVec *out);
/* OwnedWriteOperation::{Set, Delete} on the metadata column family */
groove_OwnedWriteOperation groove__db__metadata_set(jz_bytes key, jz_bytes value);
groove_OwnedWriteOperation groove__db__metadata_delete(jz_bytes key);
jz_str groove__db__metadata_cf(void);
void groove__db__WriteOpVec__drop(groove_WriteOpVec *v);

/* Weak<T> stand-in: strong-count check on a jz_rc payload. */
static inline void *groove__db__weak_upgrade(void *p) { return (p && jz_rc_strong(p) > 0) ? jz_rc_clone(p) : NULL; }

/* Arc<futures::lock::Mutex<()>>: jz_rc payload; the synchronous port holds it by flag. */
typedef struct groove_LifecycleMutex { bool locked; } groove_LifecycleMutex;
groove_LifecycleMutex *groove__db__LifecycleMutex__new(void);
void groove__db__LifecycleMutex__lock(groove_LifecycleMutex *m);
bool groove__db__LifecycleMutex__try_lock(groove_LifecycleMutex *m);
void groove__db__LifecycleMutex__unlock(groove_LifecycleMutex *m);

/* ---------------- storage_helpers.rs ---------------- */
typedef struct groove_StorageWriteBucket { u32 count; u32 bytes; } groove_StorageWriteBucket;
typedef struct groove_StorageWriteMetrics {
    groove_StorageWriteBucket total, history_rows, history_indexes, global_current_rows, global_current_indexes,
        register_global_current_rows, global_changes_rows, global_changes_indexes, transactions_rows,
        transactions_indexes, other;
} groove_StorageWriteMetrics;
typedef struct groove_StorageReadBucket { u32 reads; u32 ranges; } groove_StorageReadBucket;
typedef struct groove_StorageReadMetrics {
    groove_StorageReadBucket total, history_rows, history_indexes, ahead_current_rows, global_current_rows,
        global_current_indexes, register_global_current_rows, global_changes_rows, global_changes_indexes,
        transactions_rows, transactions_indexes, other;
} groove_StorageReadMetrics;
typedef struct groove_CommitMetrics {
    u64 storage_write_time_ms;
    u64 ivm_tick_time_ms;
    u32 storage_write_count;
    u32 storage_write_bytes;
    groove_StorageWriteMetrics storage_writes;
    groove_TickMetrics *tick;
} groove_CommitMetrics;
void groove__db__CommitMetrics__Clone__clone(groove_CommitMetrics *dst, const groove_CommitMetrics *src);
void groove__db__CommitMetrics__drop(groove_CommitMetrics *m);
enum {
    GROOVE_WRITE_DEST_HISTORY_ROWS, GROOVE_WRITE_DEST_HISTORY_INDEXES, GROOVE_WRITE_DEST_GLOBAL_CURRENT_ROWS,
    GROOVE_WRITE_DEST_GLOBAL_CURRENT_INDEXES, GROOVE_WRITE_DEST_REGISTER_GLOBAL_CURRENT_ROWS,
    GROOVE_WRITE_DEST_GLOBAL_CHANGES_ROWS, GROOVE_WRITE_DEST_GLOBAL_CHANGES_INDEXES,
    GROOVE_WRITE_DEST_TRANSACTIONS_ROWS, GROOVE_WRITE_DEST_TRANSACTIONS_INDEXES, GROOVE_WRITE_DEST_OTHER
};
enum {
    GROOVE_READ_DEST_HISTORY_ROWS, GROOVE_READ_DEST_HISTORY_INDEXES, GROOVE_READ_DEST_AHEAD_CURRENT_ROWS,
    GROOVE_READ_DEST_GLOBAL_CURRENT_ROWS, GROOVE_READ_DEST_GLOBAL_CURRENT_INDEXES,
    GROOVE_READ_DEST_REGISTER_GLOBAL_CURRENT_ROWS, GROOVE_READ_DEST_GLOBAL_CHANGES_ROWS,
    GROOVE_READ_DEST_GLOBAL_CHANGES_INDEXES, GROOVE_READ_DEST_TRANSACTIONS_ROWS,
    GROOVE_READ_DEST_TRANSACTIONS_INDEXES, GROOVE_READ_DEST_OTHER
};
groove_StorageWriteMetrics groove__db__StorageWriteMetrics__from_operations(const groove_WriteOperation *ops, u32 n, const groove_IndexIdRegistry *index_ids);
void groove__db__StorageWriteMetrics__record(groove_StorageWriteMetrics *m, const groove_WriteOperation *op, const groove_IndexIdRegistry *index_ids);
void groove__db__StorageWriteBucket__record(groove_StorageWriteBucket *b, u32 bytes);
void groove__db__StorageReadBucket__record(groove_StorageReadBucket *b, u32 reads, u32 ranges);
void groove__db__StorageReadMetrics__record_destination(groove_StorageReadMetrics *m, u8 destination, u32 reads, u32 ranges);
u32 groove__db__write_operation_bytes(const groove_WriteOperation *op);
u8 groove__db__storage_write_destination(const groove_WriteOperation *op, const groove_IndexIdRegistry *index_ids);
u8 groove__db__storage_table_write_destination(const char *table, u32 n);
u8 groove__db__storage_index_write_destination(const u8 *key, u32 n, const groove_IndexIdRegistry *index_ids);
u8 groove__db__storage_read_destination(const char *cf, u32 cfn, const u8 *key, u32 n, const groove_IndexIdRegistry *index_ids);
u8 groove__db__storage_table_read_destination(const char *table, u32 n);
u8 groove__db__storage_index_read_destination(const u8 *key, u32 n, const groove_IndexIdRegistry *index_ids);
bool groove__db__durable_index_table_and_name(const u8 *key, u32 n, const groove_IndexIdRegistry *index_ids, jz_slice *table, jz_slice *index);

/* ReadMetricsSink: Rc payload */
typedef struct groove_ReadMetricsSink { groove_StorageReadMetrics metrics; groove_SharedIndexIds index_ids; } groove_ReadMetricsSink;
groove_ReadMetricsSink *groove__db__ReadMetricsSink__new(groove_SharedIndexIds index_ids);
void groove__db__ReadMetricsSink__record_point(groove_ReadMetricsSink *s, const char *cf, u32 cfn, const u8 *key, u32 n);
void groove__db__ReadMetricsSink__record_range(groove_ReadMetricsSink *s, const char *cf, u32 cfn, const u8 *key, u32 n);
void groove__db__ReadMetricsSink__record_range_row(groove_ReadMetricsSink *s, const char *cf, u32 cfn, const u8 *key, u32 n);

/* MeteredStorage<'a, S>: LocalHandle::{Borrowed, Owned} over storage and metrics */
typedef struct groove_MeteredStorage {
    groove_DynStorage storage;
    void *storage_rc;                  /* Owned(Rc<S>) or NULL when borrowed */
    groove_ReadMetricsSink *metrics;
    bool metrics_owned;
} groove_MeteredStorage;
groove_MeteredStorage groove__db__MeteredStorage__new(groove_DynStorage storage, groove_ReadMetricsSink *metrics);
groove_MeteredStorage *groove__db__MeteredStorage__new_owned(void *storage_rc, groove_DynStorage storage, groove_ReadMetricsSink *metrics); /* Rc<MeteredStorage> */
groove_DynStorage groove__db__MeteredStorage__as_dyn(const groove_MeteredStorage *m);
void groove__db__MeteredStorage__drop(groove_MeteredStorage *m);

/* EncodedKeyValue<'a> */
typedef struct groove_EncodedKeyValue { jz_bytes key; groove_VariantRecord record; } groove_EncodedKeyValue;
typedef JZ_VEC(groove_EncodedKeyValue) groove_EncodedKeyValueVec;
typedef JZ_VEC(groove_VariantRecord) groove_VariantRecordVec;
groove_EncodedKeyValue groove__db__EncodedKeyValue__new(jz_bytes key, jz_bytes value, groove_RecordDescriptor descriptor);
groove_EncodedKeyValue groove__db__EncodedKeyValue__from_variant(jz_bytes key, groove_VariantRecord record);
jz_slice groove__db__EncodedKeyValue__key(const groove_EncodedKeyValue *e);
jz_slice groove__db__EncodedKeyValue__raw(const groove_EncodedKeyValue *e);
u32 groove__db__EncodedKeyValue__variant_tag(const groove_EncodedKeyValue *e);
const groove_VariantRecord *groove__db__EncodedKeyValue__variant_record(const groove_EncodedKeyValue *e);
void groove__db__EncodedKeyValue__into_parts(groove_EncodedKeyValue e, jz_bytes *key, jz_bytes *value);
void groove__db__EncodedKeyValue__into_variant_parts(groove_EncodedKeyValue e, jz_bytes *key, groove_VariantRecord *record);
groove_BorrowedRecord groove__db__EncodedKeyValue__record(const groove_EncodedKeyValue *e);
groove_OwnedRecord groove__db__EncodedKeyValue__owned_record(groove_EncodedKeyValue e);
void groove__db__EncodedKeyValue__drop(void *e);
void groove__db__EncodedKeyValueVec__drop(groove_EncodedKeyValueVec *v);

/* DirectRecordStore<'a> */
typedef struct groove_DirectRecordStore { const groove_LayoutStorage *storage; jz_str name; groove_RecordDescriptor key; groove_RecordDescriptor value; } groove_DirectRecordStore;
typedef struct groove_DirectRecordStoreEntry { groove_ValueVec key; groove_OwnedRecord value; } groove_DirectRecordStoreEntry;
typedef JZ_VEC(groove_DirectRecordStoreEntry) groove_DirectRecordStoreEntryVec;
typedef JZ_VEC(groove_OwnedRecord) groove_OwnedRecordVec;
enum { GROOVE_DIRECT_WRITE_SET = 0, GROOVE_DIRECT_WRITE_DELETE = 1 };
typedef struct groove_DirectRecordStoreWrite { u8 tag; groove_ValueVec key; groove_ValueVec value; } groove_DirectRecordStoreWrite;
void groove__db__DirectRecordStore__drop(groove_DirectRecordStore *s);
jz_err groove__db__DirectRecordStore__encode_writes(const groove_DirectRecordStore *s, const groove_DirectRecordStoreWrite *ops, u32 n, groove_WriteOpVec *out);
jz_err groove__db__DirectRecordStore__key_bytes(const groove_DirectRecordStore *s, const groove_Value *values, u32 n, jz_bytes *out);
jz_err groove__db__DirectRecordStore__key_prefix_bytes(const groove_DirectRecordStore *s, const groove_Value *values, u32 n, jz_bytes *out);
const jz_str *groove__db__DirectRecordStore__name(const groove_DirectRecordStore *s);
const groove_RecordDescriptor *groove__db__DirectRecordStore__key_descriptor(const groove_DirectRecordStore *s);
const groove_RecordDescriptor *groove__db__DirectRecordStore__value_descriptor(const groove_DirectRecordStore *s);
jz_err groove__db__DirectRecordStore__set(const groove_DirectRecordStore *s, const groove_Value *key, u32 kn, const groove_Value *value, u32 vn);
jz_err groove__db__DirectRecordStore__get(const groove_DirectRecordStore *s, const groove_Value *key, u32 kn, bool *found, groove_OwnedRecord *out);
jz_err groove__db__DirectRecordStore__delete(const groove_DirectRecordStore *s, const groove_Value *key, u32 kn);
jz_err groove__db__DirectRecordStore__range(const groove_DirectRecordStore *s, const groove_Value *start, u32 sn, const groove_Value *end, u32 en, groove_OwnedRecordVec *out);
jz_err groove__db__DirectRecordStore__range_entries(const groove_DirectRecordStore *s, const groove_Value *start, u32 sn, const groove_Value *end, u32 en, groove_DirectRecordStoreEntryVec *out);
jz_err groove__db__DirectRecordStore__prefix(const groove_DirectRecordStore *s, const groove_Value *prefix, u32 n, groove_OwnedRecordVec *out);
jz_err groove__db__DirectRecordStore__prefix_entries(const groove_DirectRecordStore *s, const groove_Value *prefix, u32 n, groove_DirectRecordStoreEntryVec *out);
jz_err groove__db__DirectRecordStore__write_many(const groove_DirectRecordStore *s, const groove_DirectRecordStoreWrite *ops, u32 n);
groove_RecordStore groove__db__DirectRecordStore__record_store(const groove_DirectRecordStore *s);
jz_err groove__db__DirectRecordStore__decode_key(const groove_DirectRecordStore *s, const u8 *key, u32 n, groove_ValueVec *out);

/* db::PreparedShape */
typedef struct groove_PreparedShape { groove_PreparedShapeId id; groove_QueryParameterVec parameters; groove_RecordDescriptor output; } groove_PreparedShape;
void groove__db__PreparedShape__drop(groove_PreparedShape *s);
groove_PreparedShapeId groove__db__PreparedShape__id(const groove_PreparedShape *s);
const groove_QueryParameter *groove__db__PreparedShape__parameters(const groove_PreparedShape *s, u32 *len);
const groove_RecordDescriptor *groove__db__PreparedShape__output(const groove_PreparedShape *s);

/* PendingTableWrite / WriteMode */
enum { GROOVE_PENDING_SET = 0, GROOVE_PENDING_DELETE = 1 };
enum { GROOVE_WRITE_MODE_INSERT = 0, GROOVE_WRITE_MODE_INSERT_FRESH = 1, GROOVE_WRITE_MODE_UPDATE = 2 };
typedef struct groove_PendingTableWrite {
    u8 tag;
    u8 mode;                         /* Set */
    jz_str table;
    jz_bytes key;
    u32 variant_tag;                 /* Set */
    groove_RecordDescriptor descriptor;
    jz_bytes record;                 /* Set */
} groove_PendingTableWrite;
typedef JZ_VEC(groove_PendingTableWrite) groove_PendingTableWriteVec;
void groove__db__PendingTableWrite__drop(void *w);
bool groove__db__PendingTableWrite__stored_record(const groove_PendingTableWrite *w, jz_bytes *out);
/* compute_table_deltas(pending_writes, stores, schema) */
jz_err groove__db__compute_table_deltas(const groove_PendingTableWrite *writes, const groove_RecordStore *stores, u32 n, const groove_DatabaseSchema *schema, groove_TableDeltaVec *out);

/* ---------------- batch.rs ---------------- */
enum { GROOVE_RECORD_INPUT_VALUES = 0, GROOVE_RECORD_INPUT_RECORD = 1 };
typedef struct groove_RecordInput { u8 tag; groove_ValueVec values; groove_VariantRecord record; } groove_RecordInput;
enum { GROOVE_RAW_INPUT_PAYLOAD = 0, GROOVE_RAW_INPUT_RECORD = 1, GROOVE_RAW_INPUT_VALIDATED = 2 };
typedef struct groove_RawRecordInput { u8 tag; jz_bytes payload; groove_VariantRecord record; /* Record / ValidatedRecord */ } groove_RawRecordInput;
enum { GROOVE_PKV_U8, GROOVE_PKV_U16, GROOVE_PKV_U32, GROOVE_PKV_U64, GROOVE_PKV_BOOL, GROOVE_PKV_STRING, GROOVE_PKV_BYTES, GROOVE_PKV_UUID, GROOVE_PKV_COMPOSITE };
typedef struct groove_PrimaryKeyValue groove_PrimaryKeyValue;
struct groove_PrimaryKeyValue {
    u8 tag;
    union {
        u8 u8_; u16 u16_; u32 u32_; u64 u64_; bool b; jz_str s; jz_bytes bytes; jz_uuid uuid;
        JZ_VEC(groove_PrimaryKeyValue) composite;
    } u;
};
void groove__db__batch__PrimaryKeyValue__Clone__clone(groove_PrimaryKeyValue *dst, const groove_PrimaryKeyValue *src);
void groove__db__batch__PrimaryKeyValue__drop(groove_PrimaryKeyValue *v);
jz_bytes groove__db__batch__PrimaryKeyValue__into_bytes(groove_PrimaryKeyValue self);
enum { GROOVE_BATCH_INSERT, GROOVE_BATCH_INSERT_RAW, GROOVE_BATCH_INSERT_RAW_FRESH, GROOVE_BATCH_UPDATE, GROOVE_BATCH_UPDATE_RAW, GROOVE_BATCH_DELETE };
typedef struct groove_BatchOperation {
    u8 tag;
    jz_str table;
    groove_RecordInput record;        /* Insert / Update */
    groove_PrimaryKeyValue key;       /* *Raw* / Delete */
    groove_RawRecordInput raw;        /* *Raw* */
} groove_BatchOperation;
typedef JZ_VEC(groove_BatchOperation) groove_BatchOperationVec;
void groove__db__batch__BatchOperation__drop(void *op);
const jz_str *groove__db__batch__BatchOperation__table(const groove_BatchOperation *op);
void groove__db__batch__RecordInput__drop(groove_RecordInput *r);
void groove__db__batch__RawRecordInput__drop(groove_RawRecordInput *r);
enum { GROOVE_NOTIFY_IMMEDIATE = 0, GROOVE_NOTIFY_AFTER_PERSISTENCE = 1 };
enum { GROOVE_ENSURE_EXACT_INSERTED = 0, GROOVE_ENSURE_EXACT_ALREADY_IDENTICAL = 1, GROOVE_ENSURE_EXACT_CONFLICT = 2 };
typedef struct groove_PreparedBatchWrites { void *owner; /* Option<Rc<()>> */ groove_PendingTableWriteVec writes; } groove_PreparedBatchWrites;
typedef struct groove_DatabaseBatch {
    groove_BatchOperationVec operations;
    bool has_exact_base; u32 exact_base_owner; u64 exact_base_publication;
    bool exact_conflict;
    void *exact_owner;                    /* Option<Rc<()>> */
    jz_btree exact_keys;                  /* BTreeMap<(String, Vec<u8>), Vec<u8>> */
    groove_StagedWriteState *txn_operations; /* RefCell<StagedWriteState> (lazily created) */
    u32 txn_indexed_operations;           /* Cell<usize> */
    groove_PreparedBatchWrites prepared;  /* RefCell */
    u8 notification_timing;
    groove_StagedLargeValueIdVec accepted_large_values;
    jz_btree written_tables;              /* BTreeSet<String> */
} groove_DatabaseBatch;
typedef struct groove_ExactKey { jz_str table; jz_bytes key; } groove_ExactKey;
extern const jz_btree_desc groove__db__EXACT_KEYS_DESC;
extern const jz_btree_desc groove__db__STR_SET_DESC;
void groove__db__batch__DatabaseBatch__default(groove_DatabaseBatch *b);
void groove__db__batch__DatabaseBatch__drop(groove_DatabaseBatch *b);
groove_StagedWriteState *groove__db__batch__DatabaseBatch__txn_operations(groove_DatabaseBatch *b);
void groove__db__batch__DatabaseBatch__push_operation(groove_DatabaseBatch *b, groove_BatchOperation op);
bool groove__db__batch__DatabaseBatch__writes_table(const groove_DatabaseBatch *b, const char *table, u32 n);
struct groove_Database;
jz_err groove__db__batch__DatabaseBatch__check_exact_base(const groove_DatabaseBatch *b, const struct groove_Database *db);
jz_err groove__db__batch__DatabaseBatch__ensure_exact(groove_DatabaseBatch *self, const struct groove_Database *database, jz_str table, groove_PrimaryKeyValue key,
                                                      groove_RawRecordInput record, u8 *out);
void groove__db__batch__DatabaseBatch__deliver_notifications(groove_DatabaseBatch *b, u8 timing);
void groove__db__batch__DatabaseBatch__reserve(groove_DatabaseBatch *b, u32 additional);
void groove__db__batch__DatabaseBatch__accept_large_value(groove_DatabaseBatch *b, groove_StagedLargeValueId id);
void groove__db__batch__DatabaseBatch__insert(groove_DatabaseBatch *b, jz_str table, groove_RecordInput record);
void groove__db__batch__DatabaseBatch__insert_raw(groove_DatabaseBatch *b, jz_str table, groove_PrimaryKeyValue key, groove_RawRecordInput record);
void groove__db__batch__DatabaseBatch__insert_raw_fresh(groove_DatabaseBatch *b, jz_str table, groove_PrimaryKeyValue key, groove_RawRecordInput record);
void groove__db__batch__DatabaseBatch__update(groove_DatabaseBatch *b, jz_str table, groove_RecordInput record);
void groove__db__batch__DatabaseBatch__update_raw(groove_DatabaseBatch *b, jz_str table, groove_PrimaryKeyValue key, groove_RawRecordInput record);
void groove__db__batch__DatabaseBatch__delete(groove_DatabaseBatch *b, jz_str table, groove_PrimaryKeyValue key);
bool groove__db__batch__DatabaseBatch__is_empty(const groove_DatabaseBatch *b);

/* ---------------- mod.rs: publication lifecycle ---------------- */
enum { GROOVE_APPLIED_APPLIED, GROOVE_APPLIED_PERSISTING, GROOVE_APPLIED_PERSISTENCE_COMPLETE, GROOVE_APPLIED_FINISHED, GROOVE_APPLIED_ABANDONED };
/* Rc<RefCell<PersistenceOrder>>: jz_rc payload */
typedef struct groove_PersistenceOrder {
    bool has_owner_waiter; jz_waker owner_waiter;
    u64 next;
    jz_btree waiters;      /* BTreeMap<u64, Waker> */
    bool has_failure; jz_str failure;
} groove_PersistenceOrder;
groove_PersistenceOrder *groove__db__PersistenceOrder__new(void);
void groove__db__fail_publication_order(groove_PersistenceOrder *order, jz_str message);
void groove__db__wake_publication_owner(groove_PersistenceOrder *order);

typedef struct groove_LayoutStorageCell { groove_LayoutStorage *storage; } groove_LayoutStorageCell; /* Rc<RefCell<Option<Rc<LayoutStorage>>>> */
typedef struct groove_AppliedBatch {
    groove_PublicationId publication;
    groove_LayoutStorageCell *storage;
    groove_StagedWriteState *operations;   /* Rc<RefCell<StagedWriteState>> */
    bool *resident_install_durable;        /* Option<Rc<Cell<bool>>> */
    groove_PersistenceOrder *order;
    u64 ivm_tick_time_ms;
    groove_TickMetrics *tick;
    groove_StrVec changed_tables;
    bool notifications_deferred;
    u8 *lifecycle;                         /* Rc<Cell<AppliedBatchLifecycle>> */
    bool *abandoned_application;           /* Rc<Cell<bool>> */
    groove_SharedIndexIds index_ids;
} groove_AppliedBatch;
typedef struct groove_PersistenceReceipt {
    groove_LayoutStorageCell *storage;
    u8 *lifecycle;
    groove_PersistenceOrder *order;
    bool *abandoned_application;
} groove_PersistenceReceipt;
typedef struct groove_PersistedBatch {
    groove_PublicationId publication;
    jz_err result;
    bool notifications_deferred;
    groove_CommitMetrics metrics;
    groove_PersistenceReceipt receipt;
} groove_PersistedBatch;
typedef struct groove_HostApplicationGuard { groove_PersistenceOrder *order; bool *abandoned_application; bool completed; } groove_HostApplicationGuard;
groove_PublicationId groove__db__AppliedBatch__publication(const groove_AppliedBatch *b);
const jz_str *groove__db__AppliedBatch__changed_tables(const groove_AppliedBatch *b, u32 *len);
groove_PersistedBatch groove__db__AppliedBatch__persist(const groove_AppliedBatch *b);
void groove__db__AppliedBatch__drop(groove_AppliedBatch *b);
void groove__db__HostApplicationGuard__complete(groove_HostApplicationGuard guard);
void groove__db__HostApplicationGuard__drop(groove_HostApplicationGuard *guard);
void groove__db__PersistenceReceipt__finish(const groove_PersistenceReceipt *r);
void groove__db__PersistenceReceipt__drop(groove_PersistenceReceipt *r);
void groove__db__PersistedBatch__drop(groove_PersistedBatch *b);

/* chunk install observer / journal over the metadata plane */
typedef struct groove_ResidentLifecycleInstall {
    groove_OwnedStorage storage;
    groove_StagedWriteState *staged;   /* Rc<RefCell<StagedWriteState>> */
    bool *lifecycle_held;              /* Rc<Cell<bool>> */
    bool *durable;                     /* Rc<Cell<bool>> */
    groove_PublicationInstallFailures install_failures;
} groove_ResidentLifecycleInstall;
typedef struct groove_MetadataChunkInstallObserver {
    groove_LayoutStorage *storage;     /* Weak */
    groove_LifecycleMutex *lifecycle;  /* Weak */
    bool has_resident_install; groove_ResidentLifecycleInstall resident_install;
} groove_MetadataChunkInstallObserver;
typedef struct groove_MetadataChunkInstallJournal { groove_LayoutStorage *storage; /* Weak */ } groove_MetadataChunkInstallJournal;
/* Rc::new(...) as Rc<dyn ...> */
groove_DynChunkInstallObserver groove__db__MetadataChunkInstallObserver__new_dyn(groove_LayoutStorage *storage, groove_LifecycleMutex *lifecycle,
                                                                                 const groove_ResidentLifecycleInstall *resident_install);
groove_DynChunkInstallJournal groove__db__MetadataChunkInstallJournal__new_dyn(groove_LayoutStorage *storage);

/* ---------------- Database ---------------- */
typedef struct groove_Database {
    groove_LayoutStorage *storage;                 /* Rc<LayoutStorage> */
    groove_DynChunkStorage chunk_storage;          /* Rc<dyn ChunkStorage> */
    groove_DynMissingChunkResolver chunk_resolver; /* Rc<dyn MissingChunkResolver> */
    groove_IvmRuntime *ivm_runtime;
    bool has_last_commit_metrics; groove_CommitMetrics last_commit_metrics;
    groove_TickMetrics *last_tick_metrics;         /* Option */
    groove_ReadMetricsSink *storage_read_metrics;  /* Rc */
    jz_btree stored_record_descriptors;            /* RefCell<BTreeMap<String, BTreeMap<u32, RecordDescriptor>>> */
    u64 next_publication_id;
    void *immutable_batch_owner;                   /* Rc<()> */
    void *batch_preparation_owner;                 /* Rc<()> */
    bool has_durable_publication_frontier; groove_PublicationId durable_publication_frontier;
    jz_btree resident_publications;                /* BTreeMap<PublicationId, Rc<RefCell<StagedWriteState>>> */
    jz_btree persisted_publications;               /* BTreeSet<PublicationId> */
    groove_StagedWriteState *resident_writes;      /* Rc<RefCell<StagedWriteState>> */
    groove_PersistenceOrder *publication_persistence;
    groove_LifecycleMutex *large_value_lifecycle;  /* Arc<AsyncMutex<()>> */
    groove_LifecycleMutex *large_value_publication_lifecycle_guard; /* Option<OwnedMutexGuard<()>> */
    bool *large_value_lifecycle_held;              /* Rc<Cell<bool>> */
    jz_btree large_value_lifecycle_publications;   /* BTreeSet<PublicationId> */
    bool *abandoned_application;                   /* Rc<Cell<bool>> */
    bool poisoned;
    bool storage_extraction_prepared;
} groove_Database;
typedef struct groove_RuntimeRegistryCheckpoint { groove_IvmRuntime *ivm_runtime; jz_btree stored_record_descriptors; } groove_RuntimeRegistryCheckpoint;
extern const jz_btree_desc groove__db__STORED_DESCRIPTORS_DESC; /* String -> BTreeMap<u32, RecordDescriptor> */
extern const jz_btree_desc groove__db__VARIANT_DESCRIPTORS_DESC; /* u32 -> RecordDescriptor */
extern const jz_btree_desc groove__db__PUBLICATION_STATES_DESC;  /* u64 -> Rc<RefCell<StagedWriteState>> */
extern const jz_btree_desc groove__db__U64_SET_DESC;
void groove__db__Database__drop(groove_Database *db);
void *groove__db__unit_rc_new(void); /* Rc::new(()) */
bool *groove__db__cell_bool_new(bool v); /* Rc::new(Cell::new(v)) */

/* ---------------- commit.rs ---------------- */
jz_err groove__db__commit__Database__flush(groove_Database *db);
bool groove__db__commit__Database__last_commit_metrics(const groove_Database *db, const groove_CommitMetrics **out);
const groove_TickMetrics *groove__db__commit__Database__last_tick_metrics(const groove_Database *db);
groove_StorageReadMetrics groove__db__commit__Database__storage_read_metrics(const groove_Database *db);
void groove__db__commit__Database__reset_storage_read_metrics(const groove_Database *db);
groove_StorageReadMetrics groove__db__commit__Database__take_storage_read_metrics(const groove_Database *db);
jz_err groove__db__commit__Database__apply_batch(groove_Database *db, groove_DatabaseBatch batch, groove_AppliedBatch *out);
groove_PersistedBatch groove__db__commit__Database__persist_with_progress(groove_Database *db, const groove_AppliedBatch *applied);
jz_err groove__db__commit__Database__finish_persistence(groove_Database *db, groove_PersistedBatch persistence, groove_PublicationId *out);
jz_err groove__db__commit__Database__pending_writes_from_batch(const groove_Database *db, groove_DatabaseBatch *batch, groove_PendingTableWriteVec *out);
void groove__db__commit__Database__refresh_resident_writes(groove_Database *db);
jz_err groove__db__commit__Database__ensure_batch_storage_txn(const groove_Database *db, groove_DatabaseBatch *batch);
jz_err groove__db__commit__Database__owned_storage_operation_for_pending(const groove_Database *db, const groove_PendingTableWrite *pending, groove_OwnedWriteOperation *out);
jz_err groove__db__commit__Database__pending_write_from_operation(const groove_Database *db, const groove_BatchOperation *op, groove_PendingTableWrite *out);
jz_err groove__db__commit__Database__table_storage_descriptor(const groove_Database *db, const char *table, u32 n, groove_RecordDescriptor *out);
jz_err groove__db__commit__Database__table(const groove_Database *db, const char *table, u32 n, const groove_TableSchema **out);
jz_err groove__db__commit__Database__ensure_not_poisoned(const groove_Database *db);

/* ---------------- encoding.rs ---------------- */
typedef struct groove_ResolvedRecord { u32 variant_tag; groove_RecordDescriptor descriptor; jz_bytes record; } groove_ResolvedRecord;
jz_err groove__db__encoding__resolve_record_input(const groove_TableSchema *table, const groove_RecordInput *input, groove_RecordDescriptor descriptor, groove_ResolvedRecord *out);
jz_err groove__db__encoding__resolve_raw_record_input(const groove_TableSchema *table, const groove_RawRecordInput *input, groove_RecordDescriptor descriptor, groove_ResolvedRecord *out);
jz_err groove__db__encoding__resolve_owned_record_input(const groove_TableSchema *table, groove_RecordInput input, groove_RecordDescriptor descriptor, groove_ResolvedRecord *out);
jz_err groove__db__encoding__resolve_owned_raw_record_input(const groove_TableSchema *table, groove_RawRecordInput input, groove_RecordDescriptor descriptor, groove_ResolvedRecord *out);
jz_err groove__db__encoding__resolve_variant_record(const groove_TableSchema *table, const groove_VariantRecord *record, groove_RecordDescriptor descriptor, groove_ResolvedRecord *out);
jz_err groove__db__encoding__encode_record(const groove_TableSchema *table, groove_RecordDescriptor descriptor, const groove_Value *values, u32 n, jz_bytes *out);
jz_err groove__db__encoding__primary_key_bytes(const groove_TableSchema *table, u32 variant_tag, groove_RecordDescriptor record_schema, const u8 *record, u32 n, jz_bytes *out);
jz_err groove__db__encoding__validate_raw_primary_key(const groove_TableSchema *table, u32 variant_tag, groove_RecordDescriptor record_schema, const u8 *record, u32 n, const u8 *key, u32 kn);
jz_err groove__db__encoding__Database__decode_stored_key_value(const groove_Database *db, const groove_TableSchema *table, jz_bytes key, jz_bytes stored, groove_EncodedKeyValue *out);
jz_err groove__db__encoding__persisted_index_primary_key(const groove_TableSchema *table, const char *index_name, u32 inn, const groove_IndexSchema *index,
                                                         const u8 *storage_key, u32 skn, const u8 *stored_value, u32 svn, jz_bytes *out);
jz_err groove__db__encoding__persisted_index_column_value(const groove_TableSchema *table, const char *index_name, u32 inn, const groove_IndexSchema *index,
                                                          const u8 *storage_key, u32 skn, const u8 *stored_value, u32 svn, const char *column, u32 cn, groove_Value *out);
jz_err groove__db__encoding__persisted_index_logical_key(const char *index_name, u32 inn, const u8 *storage_key, u32 skn, jz_slice *out);
jz_err groove__db__encoding__ensure_primary_key_value_type(const groove_TableSchema *table, const groove_PrimaryKeyColumn *column, const groove_Value *value);
jz_err groove__db__encoding__encode_primary_key_part(jz_bytes *key, const groove_Value *value);
void groove__db__encoding__encode_ordered_bytes(jz_bytes *key, const u8 *value, u32 n);
u64 groove__db__encoding__order_preserving_i64_bits(i64 value);
u32 groove__db__encoding__order_preserving_i32_bits(i32 value);
jz_err groove__db__encoding__decode_primary_key_part(jz_slice *bytes, const groove_ValueType *value_type, groove_Value *out);
jz_err groove__db__encoding__decode_index_key_part(jz_slice *bytes, const groove_ColumnType *column_type, const char *index_name, u32 inn, groove_Value *out);
groove_RecordDescriptor groove__db__encoding__index_record_descriptor(void);
jz_err groove__db__encoding__encode_index_prefix_part(jz_bytes *key, const groove_Value *value, const groove_ColumnType *column_type);

/* ---------------- facade.rs (used across files) ---------------- */
jz_err groove__db__facade__completed_large_value_cleanup_operations(groove_DynStorage storage, groove_StagedLargeValueId receipt_id, groove_WriteOpVec *out);
groove_StagedWriteOverlay *groove__db__facade__Database__resident_storage(const groove_Database *db);
jz_err groove__db__facade__Database__direct_record_store(const groove_Database *db, const char *name, u32 n, groove_DirectRecordStore *out);
groove_RecordStore groove__db__facade__Database__durable_indices_store_with_storage(const groove_Database *db, groove_DynStorage storage, groove_RecordDescriptor descriptor);
jz_err groove__db__facade__Database__descriptor_upload_id(const groove_LargeValueRef *value_ref, groove_StagedLargeValueId *out);

/* ---------------- primary_storage.rs (used across files) ---------------- */
jz_err groove__db__primary_storage__Database__index(const groove_Database *db, const char *table, u32 tn, const char *index, u32 in, const groove_IndexSchema **out);
jz_err groove__db__primary_storage__Database__direct_record_store_schema(const groove_Database *db, const char *store, u32 n, const groove_DirectRecordStoreSchema **out);

/* ---------------- query.rs (used across files) ---------------- */
jz_err groove__db__query__Database__drive_resident_progress_now(groove_Database *db);
jz_err groove__db__query__Database__drain_self_scheduled_resident_progress(groove_Database *db);
bool groove__db__query__Database__has_pending_progress(const groove_Database *db);
bool groove__db__query__Database__poll_progress(groove_Database *db, const jz_waker *cx, jz_err *out);

/* ---------------- schema_admission.rs (used across files) ---------------- */
jz_err groove__db__schema_admission__validate_application_storage_names(const groove_DatabaseSchema *schema);
jz_err groove__db__schema_admission__validate_durable_key_schema(const groove_DatabaseSchema *schema);
jz_err groove__db__schema_admission__validate_table_schema_variants(const groove_TableSchema *table);

/* ---- primary_storage.rs (generated prototypes) ---- */
jz_err groove__db__primary_storage__Database__table_has_stored_rows(const groove_Database *db, const char *table, u32 tn, bool *out);
jz_err groove__db__primary_storage__Database__index_prefix_entries_up_to(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                                         const groove_Value *prefix, u32 n, u32 max_items, u32 *out);
jz_err groove__db__primary_storage__Database__index_get(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                        const groove_Value *key, u32 n, groove_VariantRecordVec *out);
jz_err groove__db__primary_storage__Database__index_scan(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                         const groove_Value *prefix, u32 n, groove_VariantRecordVec *out);
jz_err groove__db__primary_storage__Database__index_scan_range(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                               const groove_Value *start, u32 sn, const groove_Value *end, u32 en, groove_VariantRecordVec *out);
jz_err groove__db__primary_storage__Database__decode_index_records(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                                   groove_EncodedKeyValueVec raw_entries, groove_VariantRecordVec *out);
jz_err groove__db__primary_storage__Database__primary_key_scan(const groove_Database *db, const char *table, u32 tn, const groove_Value *prefix, u32 n, groove_VariantRecordVec *out);
jz_err groove__db__primary_storage__Database__primary_key_scan_with_storage(const groove_Database *db, groove_DynStorage storage, const char *table, u32 tn,
                                                                            const groove_Value *prefix, u32 n, groove_VariantRecordVec *out);
jz_err groove__db__primary_storage__Database__primary_key_scan_raw(const groove_Database *db, const char *table, u32 tn, const groove_Value *prefix, u32 n, groove_EncodedKeyValueVec *out);
jz_err groove__db__primary_storage__Database__primary_key_scan_raw_in_batch(const groove_Database *db, groove_DatabaseBatch *batch, const char *table, u32 tn,
                                                                            const groove_Value *prefix, u32 n, groove_EncodedKeyValueVec *out);
jz_err groove__db__primary_storage__Database__primary_key_get_raw(const groove_Database *db, const char *table, u32 tn, const groove_Value *key, u32 n, bool *found,
                                                                  groove_EncodedKeyValue *out);
jz_err groove__db__primary_storage__Database__primary_key_get(const groove_Database *db, const char *table, u32 tn, const groove_Value *key, u32 n, bool *found,
                                                              groove_VariantRecord *out);
jz_err groove__db__primary_storage__Database__primary_key_get_raw_in_batch(const groove_Database *db, groove_DatabaseBatch *batch, const char *table, u32 tn,
                                                                           const groove_Value *key, u32 n, bool *found, groove_EncodedKeyValue *out);
jz_err groove__db__primary_storage__Database__primary_key_get_raw_with_storage(const groove_Database *db, groove_DynStorage storage, const char *table, u32 tn,
                                                                               const groove_Value *key, u32 n, bool *found, groove_EncodedKeyValue *out);
jz_err groove__db__primary_storage__Database__primary_key_scan_raw_with_storage(const groove_Database *db, groove_DynStorage storage, const char *table, u32 tn,
                                                                                const groove_Value *prefix, u32 n, groove_EncodedKeyValueVec *out);
jz_err groove__db__primary_storage__Database__primary_key_last_raw_with_storage(const groove_Database *db, groove_DynStorage storage, const char *table, u32 tn,
                                                                                const groove_Value *prefix, u32 n, bool *found, groove_EncodedKeyValue *out);
jz_err groove__db__primary_storage__Database__primary_key_scan_range_raw(const groove_Database *db, const char *table, u32 tn, const groove_Value *start, u32 sn,
                                                                         const groove_Value *end, u32 en, groove_EncodedKeyValueVec *out);
jz_err groove__db__primary_storage__Database__primary_key_scan_range_raw_in_batch(const groove_Database *db, groove_DatabaseBatch *batch, const char *table, u32 tn,
                                                                                  const groove_Value *start, u32 sn, const groove_Value *end, u32 en, groove_EncodedKeyValueVec *out);
jz_err groove__db__primary_storage__Database__primary_key_last_raw(const groove_Database *db, const char *table, u32 tn, const groove_Value *prefix, u32 n, bool *found,
                                                                   groove_EncodedKeyValue *out);
jz_err groove__db__primary_storage__Database__primary_key_last_before_or_at_raw(const groove_Database *db, const char *table, u32 tn, const groove_Value *prefix, u32 n,
                                                                                const groove_Value *upper, u32 un, bool *found, groove_EncodedKeyValue *out);
jz_err groove__db__primary_storage__Database__index_get_raw(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                            const groove_Value *key, u32 n, groove_EncodedKeyValueVec *out);
jz_err groove__db__primary_storage__Database__index_scan_raw(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                             const groove_Value *prefix, u32 n, groove_EncodedKeyValueVec *out);
jz_err groove__db__primary_storage__Database__index_scan_raw_with_storage(const groove_Database *db, groove_DynStorage storage, const char *table, u32 tn,
                                                                          const char *index_name, u32 inn, const groove_Value *prefix, u32 n, groove_EncodedKeyValueVec *out);
jz_err groove__db__primary_storage__Database__index_last_raw(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                             const groove_Value *prefix, u32 n, bool *found, groove_EncodedKeyValue *out);
jz_err groove__db__primary_storage__Database__index_scan_range_raw(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                                   const groove_Value *start, u32 sn, const groove_Value *end, u32 en, groove_EncodedKeyValueVec *out);
jz_err groove__db__primary_storage__Database__decode_raw_index_entries_with_storage(const groove_Database *db, groove_DynStorage storage, const char *table, u32 tn,
                                                                                    const char *index_name, u32 inn, groove_KvPairVec raw_entries, groove_EncodedKeyValueVec *out);
jz_err groove__db__primary_storage__Database__persisted_index_scan_prefix(const groove_Database *db, const char *table, u32 tn, const char *index_name, u32 inn,
                                                                          const groove_Value *prefix, u32 n, jz_bytes *out);

/* ---- query.rs (generated prototypes) ---- */
jz_err groove__db__query__Database__describe_template_input(const groove_Database *db, groove_GraphBuilder graph, groove_TemplateGraphInput *out);
jz_err groove__db__query__Database__graph_output_descriptor(const groove_Database *db, const groove_GraphBuilder *graph, groove_RecordDescriptor *out);
groove_InputSourceId groove__db__query__Database__allocate_input_source(groove_Database *db, groove_RecordDescriptor descriptor);
jz_err groove__db__query__Database__replace_input_sources(groove_Database *db, groove_InputSourceReplacement *items, u32 n, groove_TickMetrics **out);
jz_err groove__db__query__Database__replace_input_sources_detaching_cold(groove_Database *db, groove_InputSourceReplacement *items, u32 n, groove_TickMetrics **out);
jz_err groove__db__query__Database__apply_input_source_deltas(groove_Database *db, groove_InputSourceDelta *items, u32 n, groove_TickMetrics **out);
jz_err groove__db__query__Database__apply_input_source_deltas_detaching_cold(groove_Database *db, groove_InputSourceDelta *items, u32 n, groove_TickMetrics **out);
jz_err groove__db__query__Database__retire_input_sources(groove_Database *db, const groove_InputSourceId *ids, u32 n, groove_TickMetrics **out);
bool groove__db__query__Database__subscription_has_pending_progress(const groove_Database *db, groove_SubscriptionId subscription);
bool groove__db__query__Database__subscription_has_pending_evaluation(const groove_Database *db, groove_SubscriptionId subscription);
jz_err groove__db__query__Database__drive_progress(groove_Database *db);
jz_err groove__db__query__Database__drive_ready_progress_with_waker(groove_Database *db, const jz_waker *progress_waker);
jz_err groove__db__query__Database__drive_ready_progress(groove_Database *db);

/* ---- subscription polling ---- */
static jz_err subscription_event_result(groove_SubscriptionEvent event, groove_PublicationUpdate *out);
bool groove__db__query__Database__poll_subscription_with_publication(groove_Database *db, const groove_Subscription *subscription, const jz_waker *cx, jz_err *err,
                                                                     groove_PublicationUpdate *out);
bool groove__db__query__Database__poll_subscription(groove_Database *db, const groove_Subscription *subscription, const jz_waker *cx, jz_err *err, groove_RecordDeltas **out);
bool groove__db__query__Database__poll_multisink_subscription_with_publication(groove_Database *db, const groove_MultisinkSubscription *subscription, const jz_waker *cx,
                                                                               jz_err *err, groove_PublicationUpdate *out);
bool groove__db__query__Database__poll_multisink_subscription(groove_Database *db, const groove_MultisinkSubscription *subscription, const jz_waker *cx, jz_err *err,
                                                              groove_MultisinkDeltas **out);
jz_err groove__db__query__Database__next_subscription(groove_Database *db, const groove_Subscription *subscription, groove_RecordDeltas **out);
jz_err groove__db__query__Database__next_subscription_with_publication(groove_Database *db, const groove_Subscription *subscription, groove_PublicationUpdate *out);
jz_err groove__db__query__Database__next_multisink_subscription(groove_Database *db, const groove_MultisinkSubscription *subscription, groove_MultisinkDeltas **out);
jz_err groove__db__query__Database__next_multisink_subscription_with_publication(groove_Database *db, const groove_MultisinkSubscription *subscription,
                                                                                 groove_PublicationUpdate *out);
jz_err groove__db__query__Database__subscribe_one_sink(groove_Database *db, groove_GraphBuilder graph, groove_Subscription **out);
jz_err groove__db__query__Database__subscribe_with_lifetime_and_root_values(groove_Database *db, const jz_str *names, groove_GraphBuilder *graphs, u32 n, u8 lifetime,
                                                                            groove_RootIndirectValues root_indirect_values, const jz_waker *progress_waker,
                                                                            groove_MultisinkSubscription **out);
jz_err groove__db__query__Database__subscribe_with_lifetime(groove_Database *db, const jz_str *names, groove_GraphBuilder *graphs, u32 n, u8 lifetime,
                                                            const jz_waker *progress_waker, groove_MultisinkSubscription **out);
jz_err groove__db__query__Database__subscribe_with_waker(groove_Database *db, const jz_str *names, groove_GraphBuilder *graphs, u32 n, const jz_waker *progress_waker,
                                                         groove_MultisinkSubscription **out);
jz_err groove__db__query__Database__subscribe(groove_Database *db, const jz_str *names, groove_GraphBuilder *graphs, u32 n, groove_MultisinkSubscription **out);
jz_err groove__db__query__Database__subscribe_query(groove_Database *db, const groove_Query *query, groove_Subscription **out);
jz_err groove__db__query__Database__prepare_query(groove_Database *db, const groove_Query *query, groove_PreparedShape *out);
typedef struct groove_ShapeBinding { jz_slice name; groove_Value value; } groove_ShapeBinding;
jz_err groove__db__query__Database__bind(groove_Database *db, const groove_PreparedShape *prepared, const groove_ShapeBinding *bindings, u32 nb, groove_Subscription **out);
jz_err groove__db__query__Database__prepare_one_sink(groove_Database *db, groove_GraphBuilder graph, jz_str binding_source_shape, groove_RecordDescriptor binding_descriptor,
                                                     const jz_str *output_key_fields, u32 n, groove_IvmPreparedShape **out);
jz_err groove__db__query__Database__prepare_one_sink_with_routing(groove_Database *db, groove_GraphBuilder output_graph, groove_GraphBuilder routing_graph,
                                                                  jz_str binding_source_shape, groove_RecordDescriptor binding_descriptor, const jz_str *routing_key_fields,
                                                                  u32 n, groove_IvmPreparedShape **out);
jz_err groove__db__query__Database__prepare(groove_Database *db, groove_RoutedMultisinkTerminal **terminals, u32 n, jz_str binding_source_shape,
                                            groove_RecordDescriptor binding_descriptor, groove_IvmPreparedShape **out);
jz_err groove__db__query__Database__prepare_shared(groove_Database *db, groove_RoutedMultisinkTerminal **terminals, u32 n, jz_str binding_source_shape,
                                                   groove_RecordDescriptor binding_descriptor, groove_IvmPreparedShape **out);
void groove__db__query__Database__release_shared_prepared_shape(groove_Database *db, groove_PreparedShapeId shape);
jz_err groove__db__query__Database__bind_shape_one_sink(groove_Database *db, groove_PreparedShapeId shape, const groove_Value *values, u32 n, groove_Subscription **out);
jz_err groove__db__query__Database__bind_shape_one_sink_with_output(groove_Database *db, groove_PreparedShapeId shape, const groove_Value *values, u32 n,
                                                                    groove_RecordDescriptor public_output, groove_Subscription **out);
jz_err groove__db__query__Database__bind_shape_with_lifetime_and_root_values(groove_Database *db, groove_PreparedShapeId shape, const groove_Value *values, u32 n, u8 lifetime,
                                                                             groove_RootIndirectValues root_indirect_values, const jz_waker *progress_waker,
                                                                             groove_MultisinkSubscription **out);
jz_err groove__db__query__Database__bind_shape_with_lifetime(groove_Database *db, groove_PreparedShapeId shape, const groove_Value *values, u32 n, u8 lifetime,
                                                             const jz_waker *progress_waker, groove_MultisinkSubscription **out);
jz_err groove__db__query__Database__bind_shape_with_root_values(groove_Database *db, groove_PreparedShapeId shape, const groove_Value *values, u32 n,
                                                                groove_RootIndirectValues root_indirect_values, groove_MultisinkSubscription **out);
jz_err groove__db__query__Database__bind_shape(groove_Database *db, groove_PreparedShapeId shape, const groove_Value *values, u32 n, groove_MultisinkSubscription **out);
jz_err groove__db__query__Database__bind_shape_owned_with_root_values(groove_Database *db, groove_PreparedShapeId shape, const groove_Value *values, u32 n,
                                                                      groove_RootIndirectValues root_indirect_values, const jz_waker *progress_waker,
                                                                      groove_MultisinkSubscription **out);
jz_err groove__db__query__Database__query_graph_with_root_values(groove_Database *db, groove_GraphBuilder graph, groove_RootIndirectValues root_indirect_values,
                                                                 groove_RecordDeltas **out);
jz_err groove__db__query__Database__query_graph(groove_Database *db, groove_GraphBuilder graph, groove_RecordDeltas **out);
jz_err groove__db__query__Database__query(groove_Database *db, const groove_Query *query, groove_RecordDeltas **out);
jz_err groove__db__query__Database__query_graphs(groove_Database *db, const jz_str *names, groove_GraphBuilder *graphs, u32 n, groove_MultisinkDeltas **out);
bool groove__db__query__Database__unsubscribe(groove_Database *db, groove_SubscriptionId subscription_id);
jz_err groove__db__query__Database__retire_prepared_shape(groove_Database *db, groove_PreparedShapeId shape);
jz_err groove__db__query__Database__prune_dropped_subscriptions(groove_Database *db, u32 *out);

/* ---- schema_admission.rs (generated prototypes) ---- */
jz_err groove__db__schema_admission__Database__ensure_declared_index_generation(groove_Database *db, u64 generation);
groove_RuntimeRegistryCheckpoint groove__db__schema_admission__Database__runtime_registry_checkpoint(const groove_Database *db);
void groove__db__schema_admission__Database__restore_runtime_registry(groove_Database *db, groove_RuntimeRegistryCheckpoint checkpoint);
jz_err groove__db__schema_admission__Database__table_schema(const groove_Database *db, const char *table, u32 n, const groove_TableSchema **out);
jz_err groove__db__schema_admission__Database__register_table(groove_Database *db, groove_TableSchema table);
jz_err groove__db__schema_admission__Database__register_table_variant_with_columns(groove_Database *db, const char *table, u32 tn, const groove_ColumnSchema *columns,
                                                                                    u32 ncolumns, groove_TableVariant variant);
jz_err groove__db__schema_admission__Database__register_table_variant(groove_Database *db, const char *table, u32 tn, groove_TableVariant variant);
jz_err groove__db__schema_admission__Database__evolve_table_variant_registries(groove_Database *db, const char *table, u32 tn, const groove_ColumnSchema *columns, u32 n);
jz_err groove__db__schema_admission__Database__register_table_index(groove_Database *db, const char *table, u32 tn, groove_IndexSchema index);
jz_err groove__db__schema_admission__Database__define_variant_projection(groove_Database *db, const char *table, u32 tn, const char *target, u32 gn,
                                                                         groove_RecordDescriptor output);
jz_err groove__db__schema_admission__Database__register_variant_projection_case(groove_Database *db, const char *table, u32 tn, const char *target, u32 gn, u32 variant_tag,
                                                                                const groove_ProjectField *fields, u32 n);
jz_err groove__db__schema_admission__Database__register_variant_projection_case_omitting_unrepresentable_enums(groove_Database *db, const char *table, u32 tn,
                                                                                                               const char *target, u32 gn, u32 variant_tag,
                                                                                                               const groove_ProjectField *fields, u32 n);
jz_err groove__db__schema_admission__Database__register_variant_case(groove_Database *db, const char *table, u32 tn, const char *target, u32 gn, u32 variant_tag,
                                                                     const groove_ProjectField *fields, u32 n);
jz_err groove__db__schema_admission__Database__refresh_variant_case_for_registry_evolution(groove_Database *db, const char *table, u32 tn, const char *target, u32 gn,
                                                                                           u32 variant_tag, const groove_ProjectField *fields, u32 n);
jz_err groove__db__schema_admission__Database__register_variant_projection_enum_case(groove_Database *db, const char *table, u32 tn, const char *target, u32 gn,
                                                                                     u32 variant_tag, const char *enum_field, u32 fln, const groove_EnumSchema *enum_schema,
                                                                                     const char *case_, u32 cn, const groove_ProjectField *fields, u32 n);
jz_err groove__db__schema_admission__Database__register_variant_enum_case(groove_Database *db, const char *table, u32 tn, const char *target, u32 gn, u32 variant_tag,
                                                                          const char *enum_field, u32 fln, const groove_EnumSchema *enum_schema, const char *case_, u32 cn,
                                                                          const groove_ProjectField *fields, u32 n);
jz_err groove__db__schema_admission__Database__register_variant_projection_ignore_case(groove_Database *db, const char *table, u32 tn, const char *target, u32 gn,
                                                                                       u32 variant_tag);
jz_err groove__db__schema_admission__Database__register_variant_ignore_case(groove_Database *db, const char *table, u32 tn, const char *target, u32 gn, u32 variant_tag);
jz_err groove__db__schema_admission__Database__project_variant_record(const groove_Database *db, const char *table, u32 tn, const char *target, u32 gn,
                                                                      const groove_VariantRecord *record, bool *some, groove_OwnedRecord *out);

/* ---- facade.rs (generated prototypes) ---- */
jz_err groove__db__facade__Database__new_with_storage_layout(groove_DatabaseSchema schema, groove_BoxedStorage storage, groove_StorageLayout storage_layout, groove_Database *out);
jz_err groove__db__facade__Database__new(groove_DatabaseSchema schema, groove_BoxedStorage storage, groove_Database *out);
bool groove__db__facade__Database__durable_publication_frontier(const groove_Database *db, groove_PublicationId *out);
jz_err groove__db__facade__Database__ensure_usable(const groove_Database *db);

/* Arm cancellation/error poisoning before a host-owned durable mutation. */
jz_err groove__db__facade__Database__guard_host_application(const groove_Database *db, groove_HostApplicationGuard *out);
jz_err groove__db__facade__Database__approximate_class_bytes(const groove_Database *db, const char *cf, u32 n, bool *some, u64 *out);
jz_err groove__db__facade__Database__approximate_class_bytes_for_logical_families(const groove_Database *db, const jz_slice *families, u32 n, bool *some, u64 *out);
bool groove__db__facade__Database__has_unsettled_publications(const groove_Database *db);

/* Register the database owner's wake while external publications settle. */
jz_err groove__db__facade__Database__wait_for_publication_settlement(const groove_Database *db, const jz_waker *waker, bool *out);
jz_err groove__db__facade__Database__prepare_for_storage_extraction(groove_Database *db);
jz_err groove__db__facade__Database__rebuild(groove_Database *db, groove_DatabaseSchema schema);
groove_BoxedStorage groove__db__facade__Database__into_storage(groove_Database *db);
jz_err groove__db__facade__Database__close(const groove_Database *db);

/* Configure explicit storage durability boundaries for future committed write batches. */
jz_err groove__db__facade__Database__set_write_flush_cadence(const groove_Database *db, u32 every);
jz_err groove__db__facade__Database__flush_write_boundary(const groove_Database *db);
void groove__db__facade__Database__set_auto_direct_family_enabled(groove_Database *db, bool enabled);
void groove__db__facade__Database__set_plain_output_root_positions_enabled(groove_Database *db, bool enabled);
void groove__db__facade__Database__set_chunk_provider(groove_Database *db, groove_DynChunkProvider provider);
void groove__db__facade__Database__set_owned_chunk_provider(groove_Database *db, groove_OwnedChunkProvider provider);
groove_OwnedChunkProvider groove__db__facade__Database__owned_chunk_provider(const groove_Database *db);
groove_LocalChunkReader groove__db__facade__Database__local_chunk_reader(const groove_Database *db);
void groove__db__facade__Database__set_chunk_storage(groove_Database *db, groove_DynChunkStorage storage);
void groove__db__facade__Database__set_missing_chunk_resolver(groove_Database *db, groove_DynMissingChunkResolver resolver);
jz_err groove__db__facade__Database__stage_large_value_chunk_batch(const groove_Database *db, groove_StagedLargeValueId upload_id, u8 kind, groove_StagedChunkVec chunks);
jz_err groove__db__facade__Database__stage_large_value_chunk_batch_if_current(const groove_Database *db, groove_StagedLargeValueId upload_id, u8 kind, groove_StagedChunkVec chunks,
                                                                              bool *out);
jz_err groove__db__facade__Database__read_large_value_range(const groove_Database *db, const groove_LargeValueRef *value, u64 start, u64 end, jz_bytes *out);
jz_err groove__db__facade__Database__finalize_large_value_upload(const groove_Database *db, groove_StagedLargeValueId upload_id, groove_LargeValueRef value_ref,
                                                                 groove_StagedLargeValue *out);
jz_err groove__db__facade__Database__finalize_large_value_upload_if_current(const groove_Database *db, groove_StagedLargeValueId upload_id, groove_LargeValueRef value_ref,
                                                                            bool *some, groove_StagedLargeValue *out);
jz_err groove__db__facade__Database__evict_pending_large_value_upload(const groove_Database *db, groove_StagedLargeValueId id, bool *out);
jz_err groove__db__facade__Database__begin_large_value_upload(const groove_Database *db, const groove_LargeValueRef *value_ref, groove_LargeValueUploadProgress *out);
jz_err groove__db__facade__Database__begin_large_value_upload_with_pending_limit(const groove_Database *db, const groove_LargeValueRef *value_ref, u32 pending_limit,
                                                                                 groove_LargeValueUploadProgress *out);
jz_err groove__db__facade__Database__continue_large_value_upload(const groove_Database *db, const groove_LargeValueRef *value_ref, groove_StagedChunkVec chunks,
                                                                 groove_LargeValueUploadProgress *out);
jz_err groove__db__facade__Database__continue_large_value_upload_if_current(const groove_Database *db, const groove_LargeValueRef *value_ref, groove_StagedChunkVec chunks,
                                                                            bool *some, groove_LargeValueUploadProgress *out);
jz_err groove__db__facade__Database__stage_large_value_preparation(const groove_Database *db, groove_PreparedLargeValue prepared, groove_StagedLargeValue *out);
jz_err groove__db__facade__Database__prepare_and_stage_large_value(const groove_Database *db, u8 kind, const u8 *bytes, u32 n, groove_StagedLargeValue *out);
jz_err groove__db__facade__Database__consolidate_large_value(const groove_Database *db, groove_LargeValueRef value, groove_PreparedLargeValue *out);
jz_err groove__db__facade__Database__consolidate_and_stage_large_value(const groove_Database *db, groove_LargeValueRef value, groove_StagedLargeValue *out);
jz_err groove__db__facade__Database__append_large_value(const groove_Database *db, const groove_LargeValueRef *value, jz_bytes bytes, groove_PreparedLargeValue *out);
jz_err groove__db__facade__Database__append_and_stage_large_value(const groove_Database *db, const groove_LargeValueRef *value, jz_bytes bytes, groove_StagedLargeValue *out);
jz_err groove__db__facade__Database__edit_large_value(const groove_Database *db, const groove_LargeValueRef *value, u64 offset, u64 delete_length, jz_bytes insert_bytes,
                                                      groove_PreparedLargeValue *out);
jz_err groove__db__facade__Database__edit_and_stage_large_value(const groove_Database *db, const groove_LargeValueRef *value, u64 offset, u64 delete_length, jz_bytes insert_bytes,
                                                                groove_StagedLargeValue *out);
jz_err groove__db__facade__Database__pending_large_value_uploads(const groove_Database *db, groove_PendingLargeValueUploadVec *out);
jz_err groove__db__facade__Database__staged_large_values(const groove_Database *db, groove_StagedLargeValueVec *out);
jz_err groove__db__facade__Database__evict_staged_large_value(const groove_Database *db, groove_StagedLargeValueId id, bool *out);
jz_err groove__db__facade__Database__reclaim_orphaned_large_value_chunks(const groove_Database *db, u32 limit, u32 *out);
jz_err groove__db__facade__Database__read_large_value_cursor_next(const groove_Database *db, groove_LargeValueCursor *cursor, bool *some, jz_bytes *out);
jz_err groove__db__facade__Database__checksum_large_value_streaming(const groove_Database *db, groove_LargeValueRef value, u32 window_bytes, u32 max_bytes_per_turn,
                                                                    groove_ContentHash *hash, groove_StreamingExecutionStats *stats);
jz_err groove__db__facade__Database__read_large_text_utf16_range(const groove_Database *db, const groove_LargeValueRef *value, u64 start, u64 end, jz_str *out);
jz_err groove__db__facade__Database__large_text_utf16_offset_to_byte(const groove_Database *db, const groove_LargeValueRef *value, u64 offset, u64 *out);
jz_err groove__db__facade__Database__read_large_json_pointer(const groove_Database *db, const groove_LargeValueRef *value, const char *pointer, u32 pn, jz_json **out);
void groove__db__facade__Database__set_tick_runtime_stats_enabled(groove_Database *db, bool enabled);
groove_RuntimeStats *groove__db__facade__Database__runtime_stats(const groove_Database *db);
/* Lifetime topology-cache counters. */
groove_ExecutionLayoutStats groove__db__facade__Database__execution_layout_stats(const groove_Database *db);
void groove__db__facade__Database__open_batch(const groove_Database *db, groove_DatabaseBatch *out);
bool groove__db__facade__Database__prepared_binding_source_is_bound(const groove_Database *db, const char *shape, u32 n);
jz_err groove__db__facade__Database__write_direct_records_with_progress(groove_Database *db, const char *name, u32 n, const groove_DirectRecordStoreWrite *operations, u32 nops,
                                                                        const jz_waker *progress_waker);

#endif
