/* Mirror declarations of items owned by OTHER units that the groove-storage
 * unit (storage/, records/, large_values*, chunks, schema) calls.
 * Signatures follow the owners' headers as of writing; the coordinator
 * reconciles them at link time. */
#ifndef GROOVE_EXT_GROOVE_STORAGE_H
#define GROOVE_EXT_GROOVE_STORAGE_H
#include "jz_rt.h"

typedef struct groove_ChunkRequest groove_ChunkRequest; /* chunks.h (this unit) */

/* ---- groove::ivm::runtime (IvmRuntimeError) ----
 * IvmRuntimeError::{LargeValue, RecordEncoding, Chunk, Storage} are
 * #[error(transparent)]: their codes pass through unchanged. */
jz_err groove__ivm__runtime__IvmRuntimeError__evaluation_blocked(void);           /* Err(IvmRuntimeError::EvaluationBlocked) */
bool groove__ivm__runtime__IvmRuntimeError__is_evaluation_blocked(const jz_err *e); /* matches!(e, EvaluationBlocked) */

/* ---- groove::ivm::runtime::evaluation_session ---- */
typedef struct groove_EvaluationInputs groove_EvaluationInputs;
typedef struct groove_LoadedChunk groove_LoadedChunk;
jz_err groove__ivm__runtime__evaluation_session__EvaluationInputs__loaded_chunk(groove_EvaluationInputs *in, groove_ChunkRequest request, const groove_LoadedChunk **out);
void groove__ivm__runtime__evaluation_session__EvaluationInputs__install_chunk(groove_EvaluationInputs *in, groove_ChunkRequest request, jz_bytes bytes);
const jz_bytes *groove__ivm__runtime__evaluation_session__LoadedChunk__bytes(const groove_LoadedChunk *c);
bool groove__ivm__runtime__evaluation_session__LoadedChunk__verifies_object_hash(const groove_LoadedChunk *c, const u8 expected[32]);

/* ---- groove::db (LARGE_VALUE_METADATA_CF), owner: src/groove/db/mod.h ---- */
extern const char groove__db__LARGE_VALUE_METADATA_CF[]; /* "__groove_large_values" */

/* ---- idb_tree crate, owner: src/idb_tree/idb_tree.h ----
 * Mirrored verbatim (guarded so the owner's header may also be included). */
#ifndef IDB_TREE_H
enum {
    IDB_ERR_INVALID_OPTIONS = 0x1d00,
    IDB_ERR_MISSING_PAGE,
    IDB_ERR_INVALID_PAGE,
    IDB_ERR_PAGE_TOO_LARGE,
    IDB_ERR_STORE,
    IDB_ERR_GENERATION_CONFLICT,
    IDB_ERR_OWNERSHIP_EXPIRED,
    IDB_ERR_COMMIT_IN_FLIGHT,
    IDB_ERR_COMMIT_ABANDONED,
    IDB_ERR_NOT_RESIDENT,
    IDB_ERR_READ_ONLY_VIEW,
    IDB_STRING_ERR,
};
typedef struct idb_tree_PageStoreVtbl idb_tree_PageStoreVtbl;
typedef struct idb_tree_PageStore { const idb_tree_PageStoreVtbl *vt; void *self; } idb_tree_PageStore;
idb_tree_PageStore idb_tree__store__PageStore__clone(const idb_tree_PageStore *s);
void idb_tree__store__PageStore__drop(idb_tree_PageStore *s);
typedef struct idb_tree_Options { usize page_size; } idb_tree_Options;
void idb_tree__Options__Default__default(idb_tree_Options *out);
typedef struct idb_tree_KeyValue { jz_bytes key; jz_bytes value; } idb_tree_KeyValue;
typedef JZ_VEC(idb_tree_KeyValue) idb_tree_KeyValueVec;
void idb_tree__KeyValue__drop(void *p);
enum { IDB_WRITE_SET = 0, IDB_WRITE_DELETE = 1 };
typedef struct idb_tree_WriteOperation { u32 tag; jz_bytes key; jz_bytes value; } idb_tree_WriteOperation;
typedef JZ_VEC(idb_tree_WriteOperation) idb_tree_WriteOperationVec;
void idb_tree__WriteOperation__drop(void *p);
typedef struct idb_tree_TreeCore idb_tree_TreeCore;
typedef struct idb_tree_TreeOwnership idb_tree_TreeOwnership;
typedef struct idb_tree_IdbTree {
    idb_tree_TreeCore *inner;
    idb_tree_TreeOwnership *ownership;
    u64 *reload_epoch;
    bool read_committed;
} idb_tree_IdbTree;
jz_err idb_tree__IdbTree__open(idb_tree_PageStore store, idb_tree_Options options, idb_tree_IdbTree *out);
void idb_tree__IdbTree__read_committed(const idb_tree_IdbTree *t, idb_tree_IdbTree *out);
void idb_tree__IdbTree__clone(idb_tree_IdbTree *dst, const idb_tree_IdbTree *src);
void idb_tree__IdbTree__drop(idb_tree_IdbTree *t);
jz_err idb_tree__IdbTree__reload(const idb_tree_IdbTree *t);
/* Option<bool>: 0 None, 1 Some(false), 2 Some(true) */
jz_err idb_tree__IdbTree__value_equals(const idb_tree_IdbTree *t, jz_slice key, jz_slice expected, u8 *out);
jz_err idb_tree__IdbTree__get(const idb_tree_IdbTree *t, jz_slice key, bool *some, jz_bytes *out);
jz_err idb_tree__IdbTree__write_many(const idb_tree_IdbTree *t, idb_tree_WriteOperationVec operations);
jz_err idb_tree__IdbTree__range_limit(const idb_tree_IdbTree *t, jz_slice start, jz_slice end, usize limit, idb_tree_KeyValueVec *out);
jz_err idb_tree__IdbTree__range_reverse(const idb_tree_IdbTree *t, jz_slice start, jz_slice end, usize limit, idb_tree_KeyValueVec *out);
jz_err idb_tree__IdbTree__flush(const idb_tree_IdbTree *t);
#endif

#endif
