/* Port of crates/groove/src/db/facade.rs
 *
 * async -> synchronous. A `large_value_lifecycle.lock().await` becomes
 * LifecycleMutex lock/unlock around a `*_locked` helper so every early return
 * releases it. Test-only items (`large_value_metadata_entries_for_compatibility`,
 * `live_attaches`, `open_staged_batch`) are not ported. */
#include "jz_rt.h"
#include "../_ext_groove-ivm-db.h"
#include "mod.h"
#include <string.h>

#define LV_ERR(msg) groove__db__Error__invalid_large_value_metadata("%s", msg)
#define IS_LV_ERROR(code) (((code) & 0xFF00u) == 0x1100u)
#define IS_CHUNK_ERROR(code) (((code) & 0xFF00u) == 0x1200u)
enum { FRONTIER_LIMIT = 64 };

static void set_flag_wake(void *ctx) { *(bool *)ctx = true; }

static bool is_retryable_upload_error(const jz_err *error) {
    if (groove__db__is_storage_error(error->code)) return true; /* Error::Storage | IvmRuntime(Storage) */
    return error->code == GROOVE_EXT_CHUNK_E_BACKEND || error->code == GROOVE_EXT_CHUNK_E_UNAVAILABLE || error->code == GROOVE_EXT_CHUNK_E_RETRYABLE;
}

static inline groove_DynStorage db_storage(const groove_Database *db) { return groove__storage__LayoutStorage__as_dyn(db->storage); }

/* storage.get(LARGE_VALUE_METADATA_CF, key.clone()) */
static jz_err meta_get(groove_DynStorage storage, const jz_bytes *key, bool *found, jz_bytes *out) {
    *found = false;
    return groove__storage__OrderedKvStorage__get(storage, groove__db__metadata_cf(), jz_bytes_from(key->ptr, key->len), found, out);
}
static void push_op(groove_WriteOpVec *ops, groove_OwnedWriteOperation op) { JZ_VEC_PUSH(*ops, op); }
static void extend_ops(groove_WriteOpVec *dst, groove_WriteOpVec *src) {
    jz_vec_extend(dst, src->ptr, src->len, sizeof *src->ptr);
    jz_vec_drop(src, sizeof *src->ptr, NULL); /* elements moved */
}
static u64 now_ms(void) { return jz_now_ms(); }
static groove_StagedLargeValueId new_v4_id(void) {
    jz_uuid u = jz_uuid_v4();
    groove_StagedLargeValueId id;
    memcpy(id.b, u.b, 16);
    return id;
}
static bool accounting_eq(const groove_StagedLargeValueAccounting *a, const groove_StagedLargeValueAccounting *b) {
    return a->encoded_bytes == b->encoded_bytes && a->node_count == b->node_count;
}
static bool node_ref_eq(const groove_NodeRef *a, const groove_NodeRef *b) { return memcmp(a, b, sizeof *a) == 0; }
static bool node_vec_contains(const groove_NodeRefVec *v, const groove_NodeRef *n) {
    for (u32 i = 0; i < v->len; i++) if (node_ref_eq(&v->ptr[i], n)) return true;
    return false;
}
static void staged_chunks_drop(groove_StagedChunkVec *v) { jz_vec_drop(v, sizeof *v->ptr, groove__large_values__StagedChunk__drop); }
static void kv_batch_drop(groove_KvPairVec *v) { jz_vec_drop(v, sizeof *v->ptr, groove__storage__KvPair__drop); }

/* Build the metadata half of consuming an incomplete upload. Consumes `key`. */
static jz_err pending_large_value_upload_release_operations(groove_DynStorage storage, jz_bytes key, const groove_PendingLargeValueUpload *upload, groove_WriteOpVec *out) {
    groove_WriteOpVec operations = {0};
    push_op(&operations, groove__db__metadata_delete(key));
    jz_err e = JZ_OK;
    for (u32 i = 0; i < upload->chunks.len && !e.code; i++) {
        const groove_NodeRef *node_ref = &upload->chunks.ptr[i];
        jz_bytes node_key;
        if ((e = groove__db__large_value_node_key(node_ref, &node_key)).code) break;
        bool found;
        jz_bytes encoded = {0};
        if ((e = meta_get(storage, &node_key, &found, &encoded)).code || !found) { jz_bytes_drop(&node_key); continue; }
        groove_LargeValueNodeReferences metadata;
        e = groove__db__decode_large_value_node_references(encoded.ptr, encoded.len, &metadata);
        jz_bytes_drop(&encoded);
        if (e.code) { jz_bytes_drop(&node_key); break; }
        if (metadata.upload_references == 0) {
            e = LV_ERR("upload reference count underflow");
        } else {
            metadata.upload_references -= 1;
            jz_bytes value;
            if (!(e = groove__db__encode_large_value_node_references(&metadata, &value)).code) {
                push_op(&operations, groove__db__metadata_set(node_key, value));
                node_key = (jz_bytes){0};
                if (metadata.references == 0 && metadata.upload_references == 0) {
                    jz_bytes reclaim_key, encoded_ref;
                    if (!(e = groove__db__large_value_reclaim_key(node_ref, &reclaim_key)).code) {
                        jz_err inner = groove__large_values__encode_node_ref(node_ref, &encoded_ref);
                        if (inner.code) {
                            e = groove__db__Error__invalid_large_value_metadata("cannot encode reclaim entry: %.*s", GROOVE_DB_E(inner));
                            jz_err_drop(&inner);
                            jz_bytes_drop(&reclaim_key);
                        } else {
                            push_op(&operations, groove__db__metadata_set(reclaim_key, encoded_ref));
                        }
                    }
                }
            }
        }
        jz_bytes_drop(&node_key);
        groove__db__LargeValueNodeReferences__drop(&metadata);
    }
    if (e.code) { groove__db__WriteOpVec__drop(&operations); return e; }
    *out = operations;
    return JZ_OK;
}

/* Whether `root` holds a staging receipt or a durably published reference. */
static jz_err large_value_root_is_active(groove_DynStorage storage, const groove_NodeRef *root, bool *out) {
    jz_bytes root_key;
    JZ_TRY(groove__db__large_value_root_key(root, &root_key));
    bool found;
    jz_bytes encoded = {0};
    jz_err e = meta_get(storage, &root_key, &found, &encoded);
    jz_bytes_drop(&root_key);
    JZ_TRY(e);
    if (!found) { *out = false; return JZ_OK; }
    groove_LargeValueRootReferences references;
    e = groove__db__decode_large_value_root_references(encoded.ptr, encoded.len, &references);
    jz_bytes_drop(&encoded);
    JZ_TRY(e);
    *out = references.durable > 0 || references.staged > 0;
    return JZ_OK;
}

/* Return the staged-receipt metadata transition without committing it. Consumes `value_ref`. */
static jz_err staged_large_value_registration_operations(groove_DynStorage storage, groove_StagedLargeValueId id, groove_LargeValueRef value_ref,
                                                         groove_StagedLargeValueAccounting accounting, groove_StagedLargeValue *staged_out, groove_WriteOpVec *ops_out) {
    jz_bytes staged_key = groove__db__staged_large_value_key(id);
    bool found;
    jz_bytes encoded = {0};
    jz_err e = meta_get(storage, &staged_key, &found, &encoded);
    if (e.code) goto fail_key;
    if (found) {
        groove_StagedLargeValue existing;
        e = groove__db__decode_staged_large_value_at_key(staged_key.ptr, staged_key.len, encoded.ptr, encoded.len, &existing);
        jz_bytes_drop(&encoded);
        if (e.code) goto fail_key;
        if (groove__large_values__LargeValueRef__PartialEq__eq(&existing.value_ref, &value_ref) && accounting_eq(&existing.accounting, &accounting)) {
            *staged_out = existing;
            *ops_out = (groove_WriteOpVec){0};
            jz_bytes_drop(&staged_key);
            groove__large_values__LargeValueRef__drop(&value_ref);
            return JZ_OK;
        }
        groove__large_values__StagedLargeValue__drop(&existing);
        e = LV_ERR("staged receipt id is already bound to a different descriptor");
        goto fail_key;
    }
    groove_StagedLargeValue staged = { id, value_ref, accounting, now_ms() };
    value_ref = (groove_LargeValueRef){0};
    jz_bytes staged_encoded;
    if ((e = groove__db__encode_staged_large_value(&staged, &staged_encoded)).code) goto fail_staged;
    jz_bytes root_key;
    if ((e = groove__db__large_value_root_key(&staged.value_ref.root, &root_key)).code) { jz_bytes_drop(&staged_encoded); goto fail_staged; }
    groove_LargeValueRootReferences references = {0};
    if ((e = meta_get(storage, &root_key, &found, &encoded)).code) goto fail_root;
    if (found) {
        e = groove__db__decode_large_value_root_references(encoded.ptr, encoded.len, &references);
        jz_bytes_drop(&encoded);
        if (e.code) goto fail_root;
    }
    bool activate_root = references.durable == 0 && references.staged == 0;
    if (activate_root) references.node_active = true;
    if (__builtin_add_overflow(references.staged, 1, &references.staged)) { e = LV_ERR("staged root count overflow"); goto fail_root; }
    jz_bytes root_value;
    if ((e = groove__db__encode_large_value_root_references(&references, &root_value)).code) goto fail_root;
    groove_WriteOpVec operations = {0};
    push_op(&operations, groove__db__metadata_set(staged_key, staged_encoded));
    push_op(&operations, groove__db__metadata_set(root_key, root_value));
    if (activate_root) {
        jz_btree node_updates;
        jz_btree_init(&node_updates, &groove__db__NODE_UPDATES_DESC);
        groove_NodeTransitionVec pending = {0};
        JZ_VEC_PUSH(pending, ((groove_NodeTransition){ staged.value_ref.root, 1 }));
        groove_WriteOpVec more;
        e = groove__db__large_value_node_transition_operations(storage, node_updates, pending, false, &more);
        if (e.code) { groove__db__WriteOpVec__drop(&operations); groove__large_values__StagedLargeValue__drop(&staged); return e; }
        extend_ops(&operations, &more);
    }
    *staged_out = staged;
    *ops_out = operations;
    return JZ_OK;
fail_root:
    jz_bytes_drop(&root_key);
    jz_bytes_drop(&staged_encoded);
fail_staged:
    groove__large_values__StagedLargeValue__drop(&staged);
fail_key:
    jz_bytes_drop(&staged_key);
    groove__large_values__LargeValueRef__drop(&value_ref);
    return e;
}

/* Read one completed upload's bidirectional journals. */
static jz_err completed_large_value_upload_journal(groove_DynStorage storage, groove_StagedLargeValueId upload_id, bool *some, groove_CompletedLargeValueUpload *out) {
    *some = false;
    jz_bytes upload_key = groove__db__completed_large_value_upload_key(upload_id);
    bool found;
    jz_bytes encoded = {0};
    jz_err e = meta_get(storage, &upload_key, &found, &encoded);
    if (e.code || !found) { jz_bytes_drop(&upload_key); return e; }
    groove_CompletedLargeValueUpload completed;
    e = groove__db__decode_completed_large_value_upload_at_key(upload_key.ptr, upload_key.len, encoded.ptr, encoded.len, &completed);
    jz_bytes_drop(&encoded);
    jz_bytes_drop(&upload_key);
    JZ_TRY(e);
    jz_bytes receipt_key = groove__db__completed_large_value_receipt_key(completed.receipt_id);
    jz_bytes reverse = {0};
    e = meta_get(storage, &receipt_key, &found, &reverse);
    if (!e.code && !found) e = LV_ERR("completed upload has no receipt reverse binding");
    groove_StagedLargeValueId reverse_upload_id;
    if (!e.code) e = groove__db__decode_completed_large_value_receipt_at_key(receipt_key.ptr, receipt_key.len, reverse.ptr, reverse.len, &reverse_upload_id);
    jz_bytes_drop(&reverse);
    jz_bytes_drop(&receipt_key);
    if (!e.code && memcmp(reverse_upload_id.b, upload_id.b, 16) != 0) e = LV_ERR("completed upload bindings disagree");
    if (e.code) { groove__db__CompletedLargeValueUpload__drop(&completed); return e; }
    *out = completed;
    *some = true;
    return JZ_OK;
}

/* Resolve a completed upload only after its journal and the durable staging receipt agree. */
static jz_err completed_large_value_upload_receipt(groove_DynStorage storage, groove_StagedLargeValueId upload_id, bool *some, groove_StagedLargeValue *out) {
    groove_CompletedLargeValueUpload completed;
    JZ_TRY(completed_large_value_upload_journal(storage, upload_id, some, &completed));
    if (!*some) return JZ_OK;
    *some = false;
    groove_StagedLargeValueId receipt_id = completed.receipt_id;
    jz_bytes staged_key = groove__db__staged_large_value_key(receipt_id);
    bool found;
    jz_bytes encoded = {0};
    jz_err e = meta_get(storage, &staged_key, &found, &encoded);
    if (!e.code && !found) e = LV_ERR("completed upload points to a missing staged receipt");
    groove_StagedLargeValue staged;
    bool decoded = false;
    if (!e.code) decoded = !(e = groove__db__decode_staged_large_value_at_key(staged_key.ptr, staged_key.len, encoded.ptr, encoded.len, &staged)).code;
    jz_bytes_drop(&encoded);
    jz_bytes_drop(&staged_key);
    if (decoded && (memcmp(staged.id.b, receipt_id.b, 16) != 0 || !groove__large_values__LargeValueRef__PartialEq__eq(&completed.descriptor, &staged.value_ref) ||
                    !accounting_eq(&staged.accounting, &completed.accounting))) {
        e = LV_ERR("completed upload receipt does not match its binding");
        groove__large_values__StagedLargeValue__drop(&staged);
    }
    groove__db__CompletedLargeValueUpload__drop(&completed);
    JZ_TRY(e);
    *out = staged;
    *some = true;
    return JZ_OK;
}

static jz_err completed_large_value_upload(groove_DynStorage storage, groove_StagedLargeValueId upload_id, const groove_LargeValueRef *value_ref, bool *some,
                                           groove_StagedLargeValue *out) {
    groove_StagedLargeValue staged;
    JZ_TRY(completed_large_value_upload_receipt(storage, upload_id, some, &staged));
    if (!*some) return JZ_OK;
    if (!groove__large_values__LargeValueRef__PartialEq__eq(&staged.value_ref, value_ref)) {
        *some = false;
        groove__large_values__StagedLargeValue__drop(&staged);
        return LV_ERR("completed upload is bound to a different descriptor");
    }
    *out = staged;
    return JZ_OK;
}

jz_err groove__db__facade__completed_large_value_cleanup_operations(groove_DynStorage storage, groove_StagedLargeValueId receipt_id, groove_WriteOpVec *out) {
    *out = (groove_WriteOpVec){0};
    jz_bytes receipt_key = groove__db__completed_large_value_receipt_key(receipt_id);
    bool found;
    jz_bytes encoded = {0};
    jz_err e = meta_get(storage, &receipt_key, &found, &encoded);
    if (e.code || !found) { jz_bytes_drop(&receipt_key); return e; }
    groove_StagedLargeValueId upload_id;
    e = groove__db__decode_completed_large_value_receipt_at_key(receipt_key.ptr, receipt_key.len, encoded.ptr, encoded.len, &upload_id);
    jz_bytes_drop(&encoded);
    if (e.code) { jz_bytes_drop(&receipt_key); return e; }
    jz_bytes upload_key = groove__db__completed_large_value_upload_key(upload_id);
    jz_bytes forward = {0};
    e = meta_get(storage, &upload_key, &found, &forward);
    if (!e.code && !found) e = LV_ERR("completed receipt has no upload binding");
    groove_CompletedLargeValueUpload forward_completed;
    if (!e.code) {
        e = groove__db__decode_completed_large_value_upload_at_key(upload_key.ptr, upload_key.len, forward.ptr, forward.len, &forward_completed);
        if (!e.code) {
            if (memcmp(forward_completed.receipt_id.b, receipt_id.b, 16) != 0) e = LV_ERR("completed upload bindings disagree");
            groove__db__CompletedLargeValueUpload__drop(&forward_completed);
        }
    }
    jz_bytes_drop(&forward);
    if (e.code) { jz_bytes_drop(&upload_key); jz_bytes_drop(&receipt_key); return e; }
    push_op(out, groove__db__metadata_delete(upload_key));
    push_op(out, groove__db__metadata_delete(receipt_key));
    return JZ_OK;
}

/* ======================= impl Database ======================= */

static void install_storage_chunk_provider(groove_Database *db, groove_DynChunkStorage storage, groove_DynMissingChunkResolver resolver) {
    groove__ivm__runtime__IvmRuntime__set_chunk_provider(
        db->ivm_runtime, groove__chunks__StorageChunkProvider__with_resolver_observer_and_journal(
                             storage, resolver, groove__db__MetadataChunkInstallObserver__new_dyn(db->storage, db->large_value_lifecycle, NULL),
                             groove__db__MetadataChunkInstallJournal__new_dyn(db->storage)));
}

/* Consumes every argument. */
static void from_runtime_storage(groove_IvmRuntime *ivm_runtime, groove_LayoutStorage *storage, groove_LifecycleMutex *large_value_lifecycle,
                                 groove_DynChunkStorage chunk_storage, groove_DynMissingChunkResolver chunk_resolver, groove_Database *out) {
    memset(out, 0, sizeof *out);
    out->storage = storage;
    out->ivm_runtime = ivm_runtime;
    out->large_value_lifecycle = large_value_lifecycle;
    install_storage_chunk_provider(out, groove__chunks__DynChunkStorage__clone(&chunk_storage), groove__chunks__DynMissingChunkResolver__clone(&chunk_resolver));
    out->chunk_storage = chunk_storage;
    out->chunk_resolver = chunk_resolver;
    out->has_last_commit_metrics = false;
    out->last_tick_metrics = NULL;
    out->storage_read_metrics = groove__db__ReadMetricsSink__new(jz_rc_clone(groove__ivm__runtime__IvmRuntime__index_ids(ivm_runtime)));
    jz_btree_init(&out->stored_record_descriptors, &groove__db__STORED_DESCRIPTORS_DESC);
    out->next_publication_id = 1;
    out->immutable_batch_owner = groove__db__unit_rc_new();
    out->batch_preparation_owner = groove__db__unit_rc_new();
    out->has_durable_publication_frontier = false;
    jz_btree_init(&out->resident_publications, &groove__db__PUBLICATION_STATES_DESC);
    jz_btree_init(&out->persisted_publications, &groove__db__U64_SET_DESC);
    out->resident_writes = groove__storage__StagedWriteState__default();
    out->publication_persistence = groove__db__PersistenceOrder__new();
    out->publication_persistence->next = 1;
    out->large_value_publication_lifecycle_guard = NULL;
    out->large_value_lifecycle_held = groove__db__cell_bool_new(false);
    jz_btree_init(&out->large_value_lifecycle_publications, &groove__db__U64_SET_DESC);
    out->abandoned_application = groove__db__cell_bool_new(false);
    out->poisoned = false;
    out->storage_extraction_prepared = false;
}

/* Consumes `schema` and `storage`. */
jz_err groove__db__facade__Database__new_with_storage_layout(groove_DatabaseSchema schema, groove_BoxedStorage storage, groove_StorageLayout storage_layout, groove_Database *out) {
    /* Must precede LayoutStorage::new, whose marker initialization can create durable engine state. */
    jz_err e = groove__db__schema_admission__validate_application_storage_names(&schema);
    if (!e.code) e = groove__db__schema_admission__validate_durable_key_schema(&schema);
    if (!e.code) {
        /* Compile once against a scratch id registry so an invalid schema fails first. */
        groove_DatabaseSchema scratch;
        groove__schema__DatabaseSchema__Clone__clone(&scratch, &schema);
        groove_IvmRuntime *probe;
        e = groove__ivm__runtime__IvmRuntime__new(scratch, &probe);
        if (!e.code) groove__ivm__runtime__IvmRuntime__drop(probe);
    }
    if (e.code) {
        groove__schema__DatabaseSchema__drop(&schema);
        groove__storage__BoxedStorage__drop(&storage);
        return e;
    }
    groove_LayoutStorage *layout;
    if ((e = groove__storage__LayoutStorage__new_boxed(storage, storage_layout, &layout)).code) { groove__schema__DatabaseSchema__drop(&schema); return e; }
    /* Durable index ids must be known before schema indexes compile. */
    groove_SharedIndexIds index_ids;
    if ((e = groove__ivm__runtime__IndexIdRegistry__load(groove__storage__LayoutStorage__as_dyn(layout), &index_ids)).code) {
        groove__schema__DatabaseSchema__drop(&schema);
        jz_rc_release(layout);
        return e;
    }
    groove_IvmRuntime *ivm_runtime;
    if ((e = groove__ivm__runtime__IvmRuntime__new_with_index_ids(schema, index_ids, &ivm_runtime)).code) { jz_rc_release(layout); return e; }
    groove_DynChunkStorage chunk_storage = groove__chunks__ManagedChunkStorage__new_ordered(layout /* Rc::downgrade */);
    groove_DynMissingChunkResolver chunk_resolver = groove__chunks__UnavailableChunkResolver__new();
    from_runtime_storage(ivm_runtime, layout, groove__db__LifecycleMutex__new(), chunk_storage, chunk_resolver, out);
    return JZ_OK;
}

/* Open a schema-aware database over an ordered key/value store. */
jz_err groove__db__facade__Database__new(groove_DatabaseSchema schema, groove_BoxedStorage storage, groove_Database *out) {
    return groove__db__facade__Database__new_with_storage_layout(schema, storage, (groove_StorageLayout){ 0 /* Identity */ }, out);
}

bool groove__db__facade__Database__durable_publication_frontier(const groove_Database *db, groove_PublicationId *out) {
    if (db->has_durable_publication_frontier) *out = db->durable_publication_frontier;
    return db->has_durable_publication_frontier;
}

groove_StagedWriteOverlay *groove__db__facade__Database__resident_storage(const groove_Database *db) {
    return groove__storage__StagedWriteOverlay__new(db_storage(db), db->resident_writes);
}

/* Reject any host operation after an ambiguous durable finalization. */
jz_err groove__db__facade__Database__ensure_usable(const groove_Database *db) { return groove__db__commit__Database__ensure_not_poisoned(db); }

/* Arm cancellation/error poisoning before a host-owned durable mutation. */
jz_err groove__db__facade__Database__guard_host_application(const groove_Database *db, groove_HostApplicationGuard *out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    out->order = jz_rc_clone(db->publication_persistence);
    out->abandoned_application = jz_rc_clone(db->abandoned_application);
    out->completed = false;
    return JZ_OK;
}

jz_err groove__db__facade__Database__approximate_class_bytes(const groove_Database *db, const char *cf, u32 n, bool *some, u64 *out) {
    return groove__storage__OrderedKvStorage__approximate_class_bytes(db_storage(db), jz_str_from_slice((jz_slice){ (const u8 *)cf, n }), some, out);
}

jz_err groove__db__facade__Database__approximate_class_bytes_for_logical_families(const groove_Database *db, const jz_slice *families, u32 n, bool *some, u64 *out) {
    return groove__storage__LayoutStorage__approximate_class_bytes_for_logical_families(db->storage, families, n, some, out);
}

/* Whether an external publication owner must settle before schema teardown. */
bool groove__db__facade__Database__has_unsettled_publications(const groove_Database *db) { return db->resident_publications.len != 0; }

/* Register the database owner's wake while external publications settle. */
jz_err groove__db__facade__Database__wait_for_publication_settlement(const groove_Database *db, const jz_waker *waker, bool *out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    if (!groove__db__facade__Database__has_unsettled_publications(db)) { *out = false; return JZ_OK; }
    if (waker) {
        db->publication_persistence->owner_waiter = *waker;
        db->publication_persistence->has_owner_waiter = true;
    }
    *out = true;
    return JZ_OK;
}

/* Finish the durable obligations of a runtime that will be replaced. */
jz_err groove__db__facade__Database__prepare_for_storage_extraction(groove_Database *db) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    if (db->resident_publications.len != 0) return groove__db__Error__unit(GROOVE_DB_UnsettledPublications);
    db->poisoned = true;
    groove_OwnedStorage storage = groove__storage__OwnedStorage__new(jz_rc_clone(db->storage), db_storage(db));
    jz_err result;
    for (;;) {
        bool woken = false;
        jz_waker cx = { set_flag_wake, &woken };
        if (groove__ivm__runtime__IvmRuntime__poll_storage_extraction(db->ivm_runtime, &storage, &cx, &result)) break;
        jz_task_run_ready();
        if (!woken) jz_panic("storage extraction cannot make progress");
    }
    groove__storage__OwnedStorage__drop(&storage);
    JZ_TRY(result);
    db->storage_extraction_prepared = true;
    return JZ_OK;
}

/* Replace a successfully prepared runtime over the same storage owner. Consumes `schema`. */
jz_err groove__db__facade__Database__rebuild(groove_Database *db, groove_DatabaseSchema schema) {
    if (!db->storage_extraction_prepared) jz_panic("database rebuild requires completed preparation");
    jz_err e = groove__db__schema_admission__validate_application_storage_names(&schema);
    if (!e.code) e = groove__db__schema_admission__validate_durable_key_schema(&schema);
    if (e.code) { groove__schema__DatabaseSchema__drop(&schema); return e; }
    groove_IvmRuntime *runtime;
    JZ_TRY(groove__ivm__runtime__IvmRuntime__new_with_index_ids(schema, jz_rc_clone(groove__ivm__runtime__IvmRuntime__index_ids(db->ivm_runtime)), &runtime));
    groove__ivm__runtime__IvmRuntime__set_plain_output_root_positions_enabled(runtime, groove__ivm__runtime__IvmRuntime__plain_output_root_positions_enabled(db->ivm_runtime));
    JZ_ASSERT(db->resident_publications.len == 0, "assertion failed: self.resident_publications.is_empty()");
    JZ_ASSERT(!groove__ivm__runtime__IvmRuntime__has_pending_storage_writes(db->ivm_runtime), "assertion failed: !self.ivm_runtime.has_pending_storage_writes()");
    groove_Database replacement;
    from_runtime_storage(runtime, jz_rc_clone(db->storage), jz_rc_clone(db->large_value_lifecycle), groove__chunks__DynChunkStorage__clone(&db->chunk_storage),
                         groove__chunks__DynMissingChunkResolver__clone(&db->chunk_resolver), &replacement);
    groove__db__Database__drop(db);
    *db = replacement;
    return JZ_OK;
}

/* Consume this runtime and cancel reconstructible query work. */
groove_BoxedStorage groove__db__facade__Database__into_storage(groove_Database *db) {
    JZ_ASSERT(db->resident_publications.len == 0, "database has unsettled publications");
    JZ_ASSERT(!groove__ivm__runtime__IvmRuntime__has_pending_storage_writes(db->ivm_runtime), "database has pending durable writes");
    /* Owned cold query futures retain the layout storage; drop them first. */
    groove__ivm__runtime__IvmRuntime__drop(db->ivm_runtime);
    db->ivm_runtime = NULL;
    /* chunk storage holds only a Weak to the layout storage */
    if (jz_rc_strong(db->storage) != 1) jz_panic("database storage still has an outstanding operation");
    groove_BoxedStorage inner = groove__storage__LayoutStorage__into_inner(db->storage);
    db->storage = NULL;
    groove__db__Database__drop(db);
    return inner;
}

jz_err groove__db__facade__Database__close(const groove_Database *db) { return groove__storage__OrderedKvStorage__close(db_storage(db)); }

/* Configure explicit storage durability boundaries for future committed write batches. */
jz_err groove__db__facade__Database__set_write_flush_cadence(const groove_Database *db, u32 every) {
    return groove__storage__OrderedKvStorage__set_write_flush_cadence(db_storage(db), every);
}

/* Complete the current storage durability boundary. */
jz_err groove__db__facade__Database__flush_write_boundary(const groove_Database *db) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    return groove__storage__OrderedKvStorage__flush_write_boundary(db_storage(db));
}

void groove__db__facade__Database__set_auto_direct_family_enabled(groove_Database *db, bool enabled) {
    groove__ivm__runtime__IvmRuntime__set_auto_direct_family_enabled(db->ivm_runtime, enabled);
}
void groove__db__facade__Database__set_plain_output_root_positions_enabled(groove_Database *db, bool enabled) {
    groove__ivm__runtime__IvmRuntime__set_plain_output_root_positions_enabled(db->ivm_runtime, enabled);
}
/* Install the immutable-chunk provider used by indirect scalar evaluation. */
void groove__db__facade__Database__set_chunk_provider(groove_Database *db, groove_DynChunkProvider provider) {
    groove__ivm__runtime__IvmRuntime__set_chunk_provider(db->ivm_runtime, provider);
}
void groove__db__facade__Database__set_owned_chunk_provider(groove_Database *db, groove_OwnedChunkProvider provider) {
    groove__ivm__runtime__IvmRuntime__set_owned_chunk_provider(db->ivm_runtime, provider);
}
groove_OwnedChunkProvider groove__db__facade__Database__owned_chunk_provider(const groove_Database *db) { return groove__ivm__runtime__IvmRuntime__chunk_provider(db->ivm_runtime); }
groove_LocalChunkReader groove__db__facade__Database__local_chunk_reader(const groove_Database *db) {
    return groove__chunks__LocalChunkReader__new(groove__chunks__DynChunkStorage__clone(&db->chunk_storage));
}

/* Install Groove's policy-blind immutable chunk storage. Consumes `storage`. */
void groove__db__facade__Database__set_chunk_storage(groove_Database *db, groove_DynChunkStorage storage) {
    install_storage_chunk_provider(db, groove__chunks__DynChunkStorage__clone(&storage), groove__chunks__DynMissingChunkResolver__clone(&db->chunk_resolver));
    groove__chunks__DynChunkStorage__drop(&db->chunk_storage);
    db->chunk_storage = storage;
}

/* Consumes `resolver`. */
void groove__db__facade__Database__set_missing_chunk_resolver(groove_Database *db, groove_DynMissingChunkResolver resolver) {
    install_storage_chunk_provider(db, groove__chunks__DynChunkStorage__clone(&db->chunk_storage), groove__chunks__DynMissingChunkResolver__clone(&resolver));
    groove__chunks__DynMissingChunkResolver__drop(&db->chunk_resolver);
    db->chunk_resolver = resolver;
}

/* ---------------- large-value upload lifecycle ---------------- */

static jz_err pending_large_value_upload_limit_reached(const groove_Database *db, u32 limit, bool *out) {
    if (limit == 0) { *out = true; return JZ_OK; }
    groove_StorageCursor cursor;
    JZ_TRY(groove__storage__OrderedKvStorage__scan(db_storage(db), groove__storage__ScanRequest__prefix(groove__db__metadata_cf(), jz_bytes_from((const u8 *)"upload/", 7)), &cursor));
    u32 count = 0;
    *out = false;
    jz_err e;
    for (;;) {
        bool some;
        groove_KvPairVec batch = {0};
        if ((e = groove__storage__StorageCursor__next_batch(&cursor, &some, &batch)).code || !some) break;
        count = (count > UINT32_MAX - batch.len) ? UINT32_MAX : count + batch.len;
        kv_batch_drop(&batch);
        if (count >= limit) { *out = true; break; }
    }
    groove__storage__StorageCursor__drop(&cursor);
    return e;
}

/* Consumes `chunks`. *out = whether the batch was installed. */
static jz_err stage_chunk_batch_locked(const groove_Database *db, groove_StagedLargeValueId upload_id, u8 kind, groove_StagedChunkVec chunks, bool require_existing,
                                       bool has_pending_limit, u32 pending_limit, bool *out) {
    groove_DynStorage storage = db_storage(db);
    bool some;
    groove_StagedLargeValue completed;
    jz_err e = completed_large_value_upload_receipt(storage, upload_id, &some, &completed);
    if (e.code) goto fail_chunks;
    if (some) { groove__large_values__StagedLargeValue__drop(&completed); *out = false; goto fail_chunks; }
    /* This must precede both chunk staging and metadata mutation. */
    if ((e = groove__large_values__validate_staged_chunk_batch(kind, chunks.ptr, chunks.len)).code) goto fail_chunks;
    jz_bytes key = groove__db__pending_large_value_upload_key(upload_id);
    groove_PendingLargeValueUpload upload;
    bool found;
    jz_bytes encoded = {0};
    if ((e = meta_get(storage, &key, &found, &encoded)).code) goto fail_key;
    if (found) {
        e = groove__db__decode_pending_large_value_upload_at_key(key.ptr, key.len, encoded.ptr, encoded.len, &upload);
        jz_bytes_drop(&encoded);
        if (e.code) goto fail_key;
    } else {
        if (require_existing) { *out = false; goto fail_key; }
        if (has_pending_limit) {
            bool reached;
            if ((e = pending_large_value_upload_limit_reached(db, pending_limit, &reached)).code) goto fail_key;
            if (reached) { e = groove__db__Error__pending_upload_limit(pending_limit); goto fail_key; }
        }
        memset(&upload, 0, sizeof upload);
        upload.id = upload_id;
        upload.created_at_ms = now_ms();
    }
    jz_btree new_members;
    jz_btree_init(&new_members, &groove__db__NODE_SET_DESC);
    JZ_VEC_FOR(const groove_StagedChunk, chunk, chunks) {
        if (node_vec_contains(&upload.chunks, &chunk->node_ref)) continue;
        if (__builtin_add_overflow(upload.accounting.encoded_bytes, (u64)chunk->encoded.len, &upload.accounting.encoded_bytes)) { e = LV_ERR("upload byte count overflow"); break; }
        if (__builtin_add_overflow(upload.accounting.node_count, 1, &upload.accounting.node_count)) { e = LV_ERR("upload node count overflow"); break; }
        JZ_VEC_PUSH(upload.chunks, chunk->node_ref);
        jz_btree_insert(&new_members, &chunk->node_ref, NULL, NULL);
    }
    groove_WriteOpVec operations = {0};
    jz_bytes upload_value;
    if (!e.code && !(e = groove__db__encode_pending_large_value_upload(&upload, &upload_value)).code) {
        push_op(&operations, groove__db__metadata_set(key, upload_value));
        key = (jz_bytes){0};
    }
    for (u32 i = 0; i < chunks.len && !e.code; i++) {
        const groove_StagedChunk *chunk = &chunks.ptr[i];
        if (!jz_btree_remove(&new_members, &chunk->node_ref, NULL, NULL)) continue;
        jz_bytes node_key;
        if ((e = groove__db__large_value_node_key(&chunk->node_ref, &node_key)).code) break;
        groove_LargeValueNodeReferences metadata;
        if ((e = meta_get(storage, &node_key, &found, &encoded)).code) { jz_bytes_drop(&node_key); break; }
        if (found) {
            e = groove__db__decode_large_value_node_references(encoded.ptr, encoded.len, &metadata);
            jz_bytes_drop(&encoded);
            if (e.code) { jz_bytes_drop(&node_key); break; }
            if (__builtin_add_overflow(metadata.upload_references, 1, &metadata.upload_references)) {
                e = LV_ERR("upload reference count overflow");
                groove__db__LargeValueNodeReferences__drop(&metadata);
                jz_bytes_drop(&node_key);
                break;
            }
        } else {
            groove_ChunkNode node;
            jz_err inner = groove__large_values__decode_authenticated_node(chunk->node_ref.object_hash, chunk->encoded.ptr, chunk->encoded.len, &node);
            if (inner.code) {
                e = groove__db__Error__invalid_large_value_metadata("cannot decode pushed chunk metadata: %.*s", GROOVE_DB_E(inner));
                jz_err_drop(&inner);
                jz_bytes_drop(&node_key);
                break;
            }
            metadata = (groove_LargeValueNodeReferences){ 0, 1, groove__db__unique_large_value_children(&node) };
            groove__large_values__ChunkNode__drop(&node);
        }
        jz_bytes value;
        e = groove__db__encode_large_value_node_references(&metadata, &value);
        groove__db__LargeValueNodeReferences__drop(&metadata);
        if (e.code) { jz_bytes_drop(&node_key); break; }
        push_op(&operations, groove__db__metadata_set(node_key, value));
    }
    jz_btree_drop(&new_members);
    groove__large_values__PendingLargeValueUpload__drop(&upload);
    if (e.code) { groove__db__WriteOpVec__drop(&operations); goto fail_key; }
    /* The pending-upload record and its per-node upload references are a durable
     * intent journal committed before any separate blob backend put. */
    if ((e = groove__storage__OrderedKvStorage__write_many(storage, operations)).code) goto fail_key;
    groove_StagedLargeValueAccounting staged_accounting;
    e = db->chunk_storage.vt->stage(db->chunk_storage.self, chunks, &staged_accounting);
    jz_bytes_drop(&key);
    if (e.code) return groove__chunks__ChunkError__From__from(e);
    *out = true;
    return JZ_OK;
fail_key:
    jz_bytes_drop(&key);
fail_chunks:
    staged_chunks_drop(&chunks);
    return e;
}

static jz_err stage_large_value_chunk_batch_with_presence_and_pending_limit(const groove_Database *db, groove_StagedLargeValueId upload_id, u8 kind, groove_StagedChunkVec chunks,
                                                                            bool require_existing, bool has_pending_limit, u32 pending_limit, bool *out) {
    groove__db__LifecycleMutex__lock(db->large_value_lifecycle);
    jz_err e = stage_chunk_batch_locked(db, upload_id, kind, chunks, require_existing, has_pending_limit, pending_limit, out);
    groove__db__LifecycleMutex__unlock(db->large_value_lifecycle);
    return e;
}

/* Install one bounded batch belonging to a remote push upload. Consumes `chunks`. */
jz_err groove__db__facade__Database__stage_large_value_chunk_batch(const groove_Database *db, groove_StagedLargeValueId upload_id, u8 kind, groove_StagedChunkVec chunks) {
    bool staged;
    return stage_large_value_chunk_batch_with_presence_and_pending_limit(db, upload_id, kind, chunks, false, false, 0, &staged);
}

/* Install a batch only while the exact pending journal remains present. */
jz_err groove__db__facade__Database__stage_large_value_chunk_batch_if_current(const groove_Database *db, groove_StagedLargeValueId upload_id, u8 kind, groove_StagedChunkVec chunks,
                                                                              bool *out) {
    return stage_large_value_chunk_batch_with_presence_and_pending_limit(db, upload_id, kind, chunks, true, false, 0, out);
}

jz_err groove__db__facade__Database__descriptor_upload_id(const groove_LargeValueRef *value_ref, groove_StagedLargeValueId *out) {
    jz_bytes encoded;
    jz_err error = groove__large_values__encode_large_value_ref(value_ref, &encoded);
    if (error.code) {
        jz_err e = groove__db__Error__invalid_large_value_metadata("cannot encode upload descriptor: %.*s", GROOVE_DB_E(error));
        jz_err_drop(&error);
        return e;
    }
    u8 digest[32];
    jz_blake3_derive_key("groove pending descriptor upload v1", encoded.ptr, encoded.len, digest);
    jz_bytes_drop(&encoded);
    memcpy(out->b, digest, 16);
    return JZ_OK;
}

/* Bind a descriptor-keyed peer upload before the first authenticated frontier is disclosed. */
static jz_err bind_pending_upload_descriptor_locked(const groove_Database *db, groove_StagedLargeValueId upload_id, const groove_LargeValueRef *value_ref) {
    groove_DynStorage storage = db_storage(db);
    jz_bytes key = groove__db__pending_large_value_upload_key(upload_id);
    bool found;
    jz_bytes encoded = {0};
    jz_err e = meta_get(storage, &key, &found, &encoded);
    if (!e.code && !found) e = LV_ERR("pending upload is missing");
    if (e.code) { jz_bytes_drop(&key); return e; }
    groove_PendingLargeValueUpload upload;
    e = groove__db__decode_pending_large_value_upload_at_key(key.ptr, key.len, encoded.ptr, encoded.len, &upload);
    jz_bytes_drop(&encoded);
    if (e.code) { jz_bytes_drop(&key); return e; }
    if (upload.has_descriptor) {
        if (!groove__large_values__LargeValueRef__PartialEq__eq(&upload.descriptor, value_ref)) e = LV_ERR("pending upload is bound to a different descriptor");
        groove__large_values__PendingLargeValueUpload__drop(&upload);
        jz_bytes_drop(&key);
        return e;
    }
    upload.has_descriptor = true;
    groove__large_values__LargeValueRef__Clone__clone(&upload.descriptor, value_ref);
    jz_bytes value;
    e = groove__db__encode_pending_large_value_upload(&upload, &value);
    groove__large_values__PendingLargeValueUpload__drop(&upload);
    if (e.code) { jz_bytes_drop(&key); return e; }
    groove_WriteOpVec ops = {0};
    push_op(&ops, groove__db__metadata_set(key, value));
    return groove__storage__OrderedKvStorage__write_many(storage, ops);
}
static jz_err bind_pending_upload_descriptor(const groove_Database *db, groove_StagedLargeValueId upload_id, const groove_LargeValueRef *value_ref) {
    groove__db__LifecycleMutex__lock(db->large_value_lifecycle);
    jz_err e = bind_pending_upload_descriptor_locked(db, upload_id, value_ref);
    groove__db__LifecycleMutex__unlock(db->large_value_lifecycle);
    return e;
}

static jz_err missing_upload_frontier(const groove_Database *db, const groove_LargeValueRef *value_ref, groove_NodeRefVec *out) {
    groove_LocalChunkReader reader = groove__db__facade__Database__local_chunk_reader(db);
    jz_err e = groove__large_values__missing_upload_frontier(value_ref, &reader, FRONTIER_LIMIT, out);
    groove__chunks__LocalChunkReader__drop(&reader);
    return e;
}

/* ---- read path (needed by validation) ---- */

typedef jz_err (*attempt_fn)(void *ctx, groove_EvaluationInputs *inputs);

/* Run a chunk-demanding large-value attempt until it stops blocking. */
static jz_err attempt_with_chunks(const groove_Database *db, groove_EvaluationInputs *inputs, attempt_fn attempt, void *ctx) {
    groove_OwnedChunkProvider provider = groove__ivm__runtime__IvmRuntime__chunk_provider(db->ivm_runtime);
    jz_err e;
    for (;;) {
        e = attempt(ctx, inputs);
        if (e.code != GROOVE_EXT_IVM_EVALUATION_BLOCKED) break;
        jz_err_drop(&e);
        groove_ChunkRequestVec requests = groove__ivm__runtime__evaluation_session__EvaluationInputs__take_missing_chunks(inputs);
        if (requests.len == 0) { jz_vec_drop(&requests, sizeof *requests.ptr, NULL); e = groove__ivm__runtime__IvmRuntimeError__evaluation_blocked(); break; }
        for (u32 i = 0; i < requests.len && !e.code; i++) {
            groove_ChunkLease *bytes;
            if ((e = groove__chunks__OwnedChunkProvider__get(&provider, requests.ptr[i], &bytes)).code) break;
            groove__ivm__runtime__evaluation_session__EvaluationInputs__install_chunk_from_provider(inputs, requests.ptr[i], bytes);
        }
        jz_vec_drop(&requests, sizeof *requests.ptr, NULL);
        if (e.code) break;
    }
    groove__chunks__OwnedChunkProvider__drop(&provider);
    return e;
}

typedef struct { const groove_LargeValueRef *value; u64 start, end; jz_bytes out; } RangeAttempt;
static jz_err byte_range_attempt(void *ctx, groove_EvaluationInputs *inputs) {
    RangeAttempt *a = ctx;
    return groove__large_values__byte_range_attempt(a->value, a->start, a->end, inputs, &a->out);
}

/* Read one byte-coordinate range from the final logical scalar. */
jz_err groove__db__facade__Database__read_large_value_range(const groove_Database *db, const groove_LargeValueRef *value, u64 start, u64 end, jz_bytes *out) {
    groove_EvaluationInputs *inputs = groove__ivm__runtime__evaluation_session__EvaluationInputs__default();
    RangeAttempt a = { value, start, end, {0} };
    jz_err e = attempt_with_chunks(db, inputs, byte_range_attempt, &a);
    groove__ivm__runtime__evaluation_session__EvaluationInputs__drop(inputs);
    JZ_TRY(e);
    *out = a.out;
    return JZ_OK;
}

/* Replay a descriptor's edit tail against its immutable base tree. */
static jz_err validate_large_value_edit_tail(const groove_Database *db, const groove_LargeValueRef *value) {
    groove_EvaluationInputs *inputs = groove__ivm__runtime__evaluation_session__EvaluationInputs__default();
    groove_OwnedChunkProvider provider = groove__ivm__runtime__IvmRuntime__chunk_provider(db->ivm_runtime);
    jz_err e;
    for (;;) {
        e = groove__large_values__validate_edit_tail_attempt(value, inputs);
        if (e.code != GROOVE_EXT_IVM_EVALUATION_BLOCKED) break;
        jz_err_drop(&e);
        groove_ChunkRequestVec requests = groove__ivm__runtime__evaluation_session__EvaluationInputs__take_missing_chunks(inputs);
        if (requests.len == 0) { jz_vec_drop(&requests, sizeof *requests.ptr, NULL); e = groove__ivm__runtime__IvmRuntimeError__evaluation_blocked(); break; }
        for (u32 i = 0; i < requests.len && !e.code; i++) {
            groove_ChunkLease *bytes;
            if ((e = groove__chunks__OwnedChunkProvider__get(&provider, requests.ptr[i], &bytes)).code) break;
            groove__large_values__record_finalize_validation_bytes(groove__chunks__ChunkLease__bytes(bytes)->len);
            groove__ivm__runtime__evaluation_session__EvaluationInputs__install_chunk_from_provider(inputs, requests.ptr[i], bytes);
        }
        jz_vec_drop(&requests, sizeof *requests.ptr, NULL);
        if (e.code) break;
    }
    groove__chunks__OwnedChunkProvider__drop(&provider);
    groove__ivm__runtime__evaluation_session__EvaluationInputs__drop(inputs);
    return e;
}

/* Recheck the complete final logical scalar immediately before a remote upload becomes publishable. */
static jz_err validate_completed_large_value(const groove_Database *db, const groove_LargeValueRef *value) {
    JZ_TRY(validate_large_value_edit_tail(db, value));
    groove_LogicalValueValidator *validator;
    JZ_TRY(groove__large_values__LogicalValueValidator__new(value, &validator));
    jz_err e = JZ_OK;
    u64 offset = 0;
    while (offset < value->byte_length) {
        u64 end = offset + GROOVE_EXT_LV_LEAF_MIN_BYTES;
        if (end < offset || end > value->byte_length) end = value->byte_length;
        jz_bytes bytes;
        if ((e = groove__db__facade__Database__read_large_value_range(db, value, offset, end, &bytes)).code) break;
        groove__large_values__record_finalize_validation_bytes(bytes.len);
        e = groove__large_values__LogicalValueValidator__push(validator, bytes.ptr, bytes.len);
        jz_bytes_drop(&bytes);
        if (e.code) break;
        offset = end;
    }
    if (!e.code) e = groove__large_values__LogicalValueValidator__finish(validator, value);
    groove__large_values__LogicalValueValidator__drop(validator);
    return e;
}

/* ---- finalization ---- */

static jz_err finalize_locked(const groove_Database *db, groove_StagedLargeValueId upload_id, groove_LargeValueRef value_ref, const groove_LargeValueRef *derived_from, bool *some,
                              groove_StagedLargeValue *out) {
    groove_DynStorage storage = db_storage(db);
    *some = false;
    jz_err e = completed_large_value_upload(storage, upload_id, &value_ref, some, out);
    if (e.code || *some) { groove__large_values__LargeValueRef__drop(&value_ref); return e; }
    jz_bytes key = groove__db__pending_large_value_upload_key(upload_id);
    bool found;
    jz_bytes encoded = {0};
    if ((e = meta_get(storage, &key, &found, &encoded)).code || !found) goto fail_key;
    groove_PendingLargeValueUpload upload;
    e = groove__db__decode_pending_large_value_upload_at_key(key.ptr, key.len, encoded.ptr, encoded.len, &upload);
    jz_bytes_drop(&encoded);
    if (e.code) goto fail_key;
    jz_btree uploaded_chunks;
    jz_btree_init(&uploaded_chunks, &groove__db__NODE_SET_DESC);
    groove_PendingLargeValueUpload bound_upload;
    bool have_bound = false;
    groove_StagedWriteState *promotion_state = NULL;
    groove_StagedWriteOverlay *promotion_overlay = NULL;
    if (upload.has_descriptor && !groove__large_values__LargeValueRef__PartialEq__eq(&upload.descriptor, &value_ref)) {
        e = LV_ERR("pending upload is bound to a different descriptor");
        goto fail_upload;
    }
    JZ_VEC_FOR(const groove_NodeRef, n, upload.chunks) jz_btree_insert(&uploaded_chunks, n, NULL, NULL);
    /* JSON validity is a whole-document property; a base root that is not active
     * was never admitted here, so its validity cannot be inherited either. */
    bool derived = false;
    groove_LocalChunkReader reader = groove__db__facade__Database__local_chunk_reader(db);
    if (derived_from && upload.has_descriptor && value_ref.kind != GROOVE_LVK_JSON) {
        bool active;
        e = large_value_root_is_active(storage, &derived_from->root, &active);
        if (!e.code && active) e = groove__large_values__validate_derived_upload(&value_ref, derived_from, &reader, &uploaded_chunks, &derived);
    }
    if (!e.code) {
        if (derived) {
            e = validate_large_value_edit_tail(db, &value_ref);
        } else {
            e = groove__large_values__validate_finalized_upload(&value_ref, &reader, &uploaded_chunks, upload.has_descriptor);
            if (!e.code) e = validate_completed_large_value(db, &value_ref);
        }
    }
    groove__chunks__LocalChunkReader__drop(&reader);
    if (e.code) goto fail_upload;

    /* Persist the exact descriptor and retry receipt before promotion. */
    groove_StagedLargeValueId receipt_id = upload.has_receipt_id ? upload.receipt_id : new_v4_id();
    groove__large_values__PendingLargeValueUpload__Clone__clone(&bound_upload, &upload);
    have_bound = true;
    if (bound_upload.has_descriptor) groove__large_values__LargeValueRef__drop(&bound_upload.descriptor);
    bound_upload.has_descriptor = true;
    groove__large_values__LargeValueRef__Clone__clone(&bound_upload.descriptor, &value_ref);
    bound_upload.has_receipt_id = true;
    bound_upload.receipt_id = receipt_id;
    if (!upload.has_descriptor || !upload.has_receipt_id) {
        jz_bytes value;
        if ((e = groove__db__encode_pending_large_value_upload(&bound_upload, &value)).code) goto fail_upload;
        groove_WriteOpVec ops = {0};
        push_op(&ops, groove__db__metadata_set(jz_bytes_from(key.ptr, key.len), value));
        if ((e = groove__storage__OrderedKvStorage__write_many(storage, ops)).code) goto fail_upload;
    }

    /* Publishing the receipt and consuming its journal are one durable transition. */
    groove_WriteOpVec release_operations;
    e = pending_large_value_upload_release_operations(storage, key, &upload, &release_operations);
    key = (jz_bytes){0};
    if (e.code) goto fail_upload;
    promotion_state = groove__storage__StagedWriteState__from_ops(release_operations);
    promotion_overlay = groove__storage__StagedWriteOverlay__new(storage, promotion_state);
    groove_StagedLargeValue staged;
    groove_WriteOpVec registration_operations;
    e = staged_large_value_registration_operations(groove__storage__StagedWriteOverlay__as_dyn(promotion_overlay), receipt_id, value_ref, upload.accounting, &staged,
                                                   &registration_operations);
    value_ref = (groove_LargeValueRef){0};
    if (e.code) goto fail_upload;
    groove__storage__StagedWriteState__extend(promotion_state, registration_operations);
    groove_CompletedLargeValueUpload completed_record = { upload_id, receipt_id, staged.value_ref /* borrowed */, staged.accounting };
    jz_bytes completed;
    if ((e = groove__db__encode_completed_large_value_upload(&completed_record, &completed)).code) { groove__large_values__StagedLargeValue__drop(&staged); goto fail_upload; }
    groove_WriteOpVec journal = {0};
    push_op(&journal, groove__db__metadata_set(groove__db__completed_large_value_upload_key(upload_id), completed));
    push_op(&journal, groove__db__metadata_set(groove__db__completed_large_value_receipt_key(receipt_id), jz_bytes_from(upload_id.b, 16)));
    groove__storage__StagedWriteState__extend(promotion_state, journal);
    groove__storage__StagedWriteOverlay__release(promotion_overlay);
    promotion_overlay = NULL;
    e = groove__storage__OrderedKvStorage__write_many(storage, groove__storage__StagedWriteState__into_operations(promotion_state));
    if (e.code) { groove__large_values__StagedLargeValue__drop(&staged); goto fail_upload; }
    *out = staged;
    *some = true;
fail_upload:
    if (promotion_overlay) groove__storage__StagedWriteOverlay__release(promotion_overlay);
    if (promotion_state) jz_rc_release(promotion_state);
    if (have_bound) groove__large_values__PendingLargeValueUpload__drop(&bound_upload);
    jz_btree_drop(&uploaded_chunks);
    groove__large_values__PendingLargeValueUpload__drop(&upload);
fail_key:
    jz_bytes_drop(&key);
    groove__large_values__LargeValueRef__drop(&value_ref);
    return e;
}

/* `derived_from` NULL = UploadValidation::Full. Consumes `value_ref`. */
static jz_err finalize_large_value_upload_with_presence(const groove_Database *db, groove_StagedLargeValueId upload_id, groove_LargeValueRef value_ref,
                                                        const groove_LargeValueRef *derived_from, bool *some, groove_StagedLargeValue *out) {
    groove__db__LifecycleMutex__lock(db->large_value_lifecycle);
    jz_err e = finalize_locked(db, upload_id, value_ref, derived_from, some, out);
    groove__db__LifecycleMutex__unlock(db->large_value_lifecycle);
    return e;
}

/* Finalize a streamed push upload into an opaque persisted staging receipt. */
jz_err groove__db__facade__Database__finalize_large_value_upload(const groove_Database *db, groove_StagedLargeValueId upload_id, groove_LargeValueRef value_ref,
                                                                 groove_StagedLargeValue *out) {
    bool some;
    JZ_TRY(finalize_large_value_upload_with_presence(db, upload_id, value_ref, NULL, &some, out));
    return some ? JZ_OK : LV_ERR("pending upload is missing");
}

/* Finalize only while the exact pending journal remains present. */
jz_err groove__db__facade__Database__finalize_large_value_upload_if_current(const groove_Database *db, groove_StagedLargeValueId upload_id, groove_LargeValueRef value_ref,
                                                                            bool *some, groove_StagedLargeValue *out) {
    return finalize_large_value_upload_with_presence(db, upload_id, value_ref, NULL, some, out);
}

/* ---- eviction ---- */

static jz_err release_pending_large_value_upload(const groove_Database *db, jz_bytes key, const groove_PendingLargeValueUpload *upload) {
    groove_WriteOpVec ops;
    JZ_TRY(pending_large_value_upload_release_operations(db_storage(db), key, upload, &ops));
    return groove__storage__OrderedKvStorage__write_many(db_storage(db), ops);
}

static jz_err evict_pending_locked(const groove_Database *db, groove_StagedLargeValueId id, bool *out) {
    jz_bytes key = groove__db__pending_large_value_upload_key(id);
    bool found;
    jz_bytes encoded = {0};
    jz_err e = meta_get(db_storage(db), &key, &found, &encoded);
    if (e.code || !found) { jz_bytes_drop(&key); *out = false; return e; }
    groove_PendingLargeValueUpload upload;
    e = groove__db__decode_pending_large_value_upload_at_key(key.ptr, key.len, encoded.ptr, encoded.len, &upload);
    jz_bytes_drop(&encoded);
    if (e.code) { jz_bytes_drop(&key); return e; }
    e = release_pending_large_value_upload(db, key, &upload);
    groove__large_values__PendingLargeValueUpload__drop(&upload);
    JZ_TRY(e);
    *out = true;
    return JZ_OK;
}

/* Idempotently expire one incomplete push upload. */
jz_err groove__db__facade__Database__evict_pending_large_value_upload(const groove_Database *db, groove_StagedLargeValueId id, bool *out) {
    groove__db__LifecycleMutex__lock(db->large_value_lifecycle);
    jz_err e = evict_pending_locked(db, id, out);
    groove__db__LifecycleMutex__unlock(db->large_value_lifecycle);
    return e;
}

/* Evict the pending journal, then return `error` (consumed). */
static jz_err reject_large_value_upload(const groove_Database *db, groove_StagedLargeValueId upload_id, jz_err error) {
    bool evicted;
    jz_err e = groove__db__facade__Database__evict_pending_large_value_upload(db, upload_id, &evicted);
    if (e.code) { jz_err_drop(&error); return e; }
    return error;
}

/* ---- upload progress ---- */

static jz_err large_value_upload_progress(const groove_Database *db, groove_StagedLargeValueId upload_id, const groove_LargeValueRef *value_ref, bool require_existing,
                                          bool *some, groove_LargeValueUploadProgress *out) {
    *some = false;
    groove_NodeRefVec missing = {0};
    jz_err error = missing_upload_frontier(db, value_ref, &missing);
    if (error.code) return is_retryable_upload_error(&error) ? error : reject_large_value_upload(db, upload_id, error);
    if (missing.len != 0) {
        memset(out, 0, sizeof *out);
        out->tag = GROOVE_UPLOAD_MISSING;
        out->missing = missing;
        *some = true;
        return JZ_OK;
    }
    jz_vec_drop(&missing, sizeof(groove_NodeRef), NULL);
    if ((error = validate_completed_large_value(db, value_ref)).code)
        return is_retryable_upload_error(&error) ? error : reject_large_value_upload(db, upload_id, error);
    groove_LargeValueRef owned;
    groove__large_values__LargeValueRef__Clone__clone(&owned, value_ref);
    groove_StagedLargeValue staged;
    bool finalized = true;
    if (require_existing) {
        error = groove__db__facade__Database__finalize_large_value_upload_if_current(db, upload_id, owned, &finalized, &staged);
    } else {
        error = groove__db__facade__Database__finalize_large_value_upload(db, upload_id, owned, &staged);
    }
    if (error.code) return is_retryable_upload_error(&error) ? error : reject_large_value_upload(db, upload_id, error);
    if (!finalized) return JZ_OK;
    memset(out, 0, sizeof *out);
    out->tag = GROOVE_UPLOAD_STAGED;
    out->staged = staged;
    *some = true;
    return JZ_OK;
}

static void progress_staged(groove_LargeValueUploadProgress *out, groove_StagedLargeValue staged) {
    memset(out, 0, sizeof *out);
    out->tag = GROOVE_UPLOAD_STAGED;
    out->staged = staged;
}

static jz_err begin_large_value_upload_inner(const groove_Database *db, const groove_LargeValueRef *value_ref, bool has_pending_limit, u32 pending_limit,
                                             groove_LargeValueUploadProgress *out) {
    groove_StagedLargeValueId upload_id;
    JZ_TRY(groove__db__facade__Database__descriptor_upload_id(value_ref, &upload_id));
    bool some;
    groove_StagedLargeValue staged;
    JZ_TRY(completed_large_value_upload(db_storage(db), upload_id, value_ref, &some, &staged));
    if (some) { progress_staged(out, staged); return JZ_OK; }
    bool installed;
    JZ_TRY(stage_large_value_chunk_batch_with_presence_and_pending_limit(db, upload_id, value_ref->kind, (groove_StagedChunkVec){0}, false, has_pending_limit, pending_limit,
                                                                         &installed));
    if (!installed) {
        JZ_TRY(completed_large_value_upload(db_storage(db), upload_id, value_ref, &some, &staged));
        if (some) { progress_staged(out, staged); return JZ_OK; }
    }
    JZ_TRY(bind_pending_upload_descriptor(db, upload_id, value_ref));
    JZ_TRY(large_value_upload_progress(db, upload_id, value_ref, false, &some, out));
    return some ? JZ_OK : LV_ERR("pending upload is missing");
}

/* Start or resume a descriptor-keyed root-first upload. */
jz_err groove__db__facade__Database__begin_large_value_upload(const groove_Database *db, const groove_LargeValueRef *value_ref, groove_LargeValueUploadProgress *out) {
    return begin_large_value_upload_inner(db, value_ref, false, 0, out);
}

/* Start or resume an upload without creating more than `pending_limit` incomplete-upload records. */
jz_err groove__db__facade__Database__begin_large_value_upload_with_pending_limit(const groove_Database *db, const groove_LargeValueRef *value_ref, u32 pending_limit,
                                                                                 groove_LargeValueUploadProgress *out) {
    return begin_large_value_upload_inner(db, value_ref, true, pending_limit, out);
}

/* Consumes `chunks`. */
static jz_err continue_large_value_upload_with_presence(const groove_Database *db, const groove_LargeValueRef *value_ref, groove_StagedChunkVec chunks, bool require_existing,
                                                        bool *some, groove_LargeValueUploadProgress *out) {
    *some = false;
    groove_StagedLargeValueId upload_id;
    jz_err e = groove__db__facade__Database__descriptor_upload_id(value_ref, &upload_id);
    if (e.code) { staged_chunks_drop(&chunks); return e; }
    groove_StagedLargeValue staged;
    if ((e = completed_large_value_upload(db_storage(db), upload_id, value_ref, some, &staged)).code) { staged_chunks_drop(&chunks); return e; }
    if (*some) { staged_chunks_drop(&chunks); progress_staged(out, staged); return JZ_OK; }
    groove_NodeRefVec requested = {0};
    jz_err error = missing_upload_frontier(db, value_ref, &requested);
    if (error.code) {
        staged_chunks_drop(&chunks);
        /* A corrupt locally durable chunk is terminal; transport/backend availability remains retryable. */
        if (IS_LV_ERROR(error.code) || error.code == GROOVE_EXT_CHUNK_E_INTEGRITY) return reject_large_value_upload(db, upload_id, error);
        return error;
    }
    if (chunks.len == 0 && requested.len != 0) {
        jz_vec_drop(&requested, sizeof(groove_NodeRef), NULL);
        staged_chunks_drop(&chunks);
        return reject_large_value_upload(db, upload_id, LV_ERR("upload supplied no requested nodes"));
    }
    groove_StagedChunkVec new_chunks = {0};
    jz_vec_reserve(&new_chunks, chunks.len, sizeof(groove_StagedChunk));
    groove_LocalChunkReader reader = groove__db__facade__Database__local_chunk_reader(db);
    bool outside = false;
    for (u32 i = 0; i < chunks.len; i++) {
        groove_StagedChunk *chunk = &chunks.ptr[i];
        if (outside) { groove__large_values__StagedChunk__drop(chunk); continue; }
        if (node_vec_contains(&requested, &chunk->node_ref)) { JZ_VEC_PUSH(new_chunks, *chunk); continue; }
        /* Another connection may have satisfied the same frontier; a stale response
         * is idempotent only when it is byte-for-byte the stored node. */
        jz_bytes stored = {0};
        jz_err got = groove__chunks__LocalChunkReader__get(&reader, chunk->node_ref.locator, chunk->node_ref.object_hash, &stored);
        bool already_stored = !got.code && stored.len == chunk->encoded.len && (stored.len == 0 || memcmp(stored.ptr, chunk->encoded.ptr, stored.len) == 0);
        jz_err_drop(&got);
        jz_bytes_drop(&stored);
        groove__large_values__StagedChunk__drop(chunk);
        if (!already_stored) outside = true;
    }
    jz_vec_drop(&chunks, sizeof(groove_StagedChunk), NULL); /* elements moved or dropped */
    groove__chunks__LocalChunkReader__drop(&reader);
    jz_vec_drop(&requested, sizeof(groove_NodeRef), NULL);
    if (outside) {
        staged_chunks_drop(&new_chunks);
        return reject_large_value_upload(db, upload_id, LV_ERR("upload supplied a node outside the authenticated missing frontier"));
    }
    if ((error = groove__large_values__validate_staged_chunk_batch(value_ref->kind, new_chunks.ptr, new_chunks.len)).code) {
        staged_chunks_drop(&new_chunks);
        return reject_large_value_upload(db, upload_id, error);
    }
    bool installed = true;
    if (require_existing)
        error = groove__db__facade__Database__stage_large_value_chunk_batch_if_current(db, upload_id, value_ref->kind, new_chunks, &installed);
    else
        error = groove__db__facade__Database__stage_large_value_chunk_batch(db, upload_id, value_ref->kind, new_chunks);
    if (error.code) return is_retryable_upload_error(&error) ? error : reject_large_value_upload(db, upload_id, error);
    if (!installed) {
        JZ_TRY(completed_large_value_upload(db_storage(db), upload_id, value_ref, some, &staged));
        if (*some) progress_staged(out, staged);
        return JZ_OK;
    }
    JZ_TRY(bind_pending_upload_descriptor(db, upload_id, value_ref));
    return large_value_upload_progress(db, upload_id, value_ref, require_existing, some, out);
}

/* Install receiver-requested nodes and derive the next missing frontier. Consumes `chunks`. */
jz_err groove__db__facade__Database__continue_large_value_upload(const groove_Database *db, const groove_LargeValueRef *value_ref, groove_StagedChunkVec chunks,
                                                                 groove_LargeValueUploadProgress *out) {
    bool some;
    JZ_TRY(continue_large_value_upload_with_presence(db, value_ref, chunks, false, &some, out));
    return some ? JZ_OK : LV_ERR("pending upload is missing");
}

/* Continue a descriptor-bound upload only while its original pending journal remains present. */
jz_err groove__db__facade__Database__continue_large_value_upload_if_current(const groove_Database *db, const groove_LargeValueRef *value_ref, groove_StagedChunkVec chunks,
                                                                            bool *some, groove_LargeValueUploadProgress *out) {
    return continue_large_value_upload_with_presence(db, value_ref, chunks, true, some, out);
}

/* ---- preparation / staging ---- */

/* Persist all immutable nodes emitted by a Groove preparation. Consumes `prepared`. */
jz_err groove__db__facade__Database__stage_large_value_preparation(const groove_Database *db, groove_PreparedLargeValue prepared, groove_StagedLargeValue *out) {
    groove_StagedLargeValueId upload_id = new_v4_id();
    jz_err e = groove__db__facade__Database__stage_large_value_chunk_batch(db, upload_id, prepared.value_ref.kind, prepared.staged_chunks);
    if (e.code) { groove__large_values__LargeValueRef__drop(&prepared.value_ref); return e; }
    return groove__db__facade__Database__finalize_large_value_upload(db, upload_id, prepared.value_ref, out);
}

/* Prepare and stage a complete logical value entirely inside Groove. */
jz_err groove__db__facade__Database__prepare_and_stage_large_value(const groove_Database *db, u8 kind, const u8 *bytes, u32 n, groove_StagedLargeValue *out) {
    groove_PreparedLargeValue prepared;
    JZ_TRY(groove__large_values__prepare(kind, bytes, n, &prepared)); /* IvmRuntimeError::from: transparent */
    return groove__db__facade__Database__stage_large_value_preparation(db, prepared, out);
}

/* Stage a descriptor Groove derived from an already authenticated local value. Consumes `prepared`. */
static jz_err stage_derived_large_value_preparation(const groove_Database *db, const groove_LargeValueRef *base, groove_PreparedLargeValue prepared, groove_StagedLargeValue *out) {
    groove_StagedLargeValueId upload_id = new_v4_id();
    jz_err e = groove__db__facade__Database__stage_large_value_chunk_batch(db, upload_id, prepared.value_ref.kind, prepared.staged_chunks);
    if (!e.code) e = bind_pending_upload_descriptor(db, upload_id, &prepared.value_ref);
    if (e.code) { groove__large_values__LargeValueRef__drop(&prepared.value_ref); return e; }
    bool some;
    JZ_TRY(finalize_large_value_upload_with_presence(db, upload_id, prepared.value_ref, base, &some, out));
    return some ? JZ_OK : LV_ERR("pending upload is missing");
}

typedef struct { groove_ConsolidationContinuation *c; bool has; groove_PreparedLargeValue out; } StepAttempt;
static jz_err consolidation_step(void *ctx, groove_EvaluationInputs *inputs) {
    StepAttempt *a = ctx;
    return groove__large_values__ConsolidationContinuation__step(a->c, inputs, &a->has, &a->out);
}

/* Consolidate a bounded edit tail inside one Groove-owned resumable preparation. Consumes `value`. */
jz_err groove__db__facade__Database__consolidate_large_value(const groove_Database *db, groove_LargeValueRef value, groove_PreparedLargeValue *out) {
    StepAttempt a = {0};
    JZ_TRY(groove__large_values__ConsolidationContinuation__new(value, &a.c));
    groove_EvaluationInputs *inputs = groove__ivm__runtime__evaluation_session__EvaluationInputs__default();
    jz_err e;
    for (;;) {
        a.has = false;
        if ((e = attempt_with_chunks(db, inputs, consolidation_step, &a)).code) break;
        if (a.has) { *out = a.out; break; }
    }
    groove__ivm__runtime__evaluation_session__EvaluationInputs__drop(inputs);
    groove__large_values__ConsolidationContinuation__drop(a.c);
    return e;
}

/* Consolidate and persist every newly emitted immutable node inside Groove. Consumes `value`. */
jz_err groove__db__facade__Database__consolidate_and_stage_large_value(const groove_Database *db, groove_LargeValueRef value, groove_StagedLargeValue *out) {
    groove_LargeValueRef copy;
    groove__large_values__LargeValueRef__Clone__clone(&copy, &value);
    groove_PreparedLargeValue prepared;
    jz_err e = groove__db__facade__Database__consolidate_large_value(db, copy, &prepared);
    if (!e.code) e = stage_derived_large_value_preparation(db, &value, prepared, out);
    groove__large_values__LargeValueRef__drop(&value);
    return e;
}

static void prepared_from_tail(groove_TailOutcome outcome, groove_PreparedLargeValue *out) {
    out->value_ref = outcome.value;
    out->staged_chunks = (groove_StagedChunkVec){0};
}

/* Prepare an append using the bounded edit tail. Consumes `bytes`. */
jz_err groove__db__facade__Database__append_large_value(const groove_Database *db, const groove_LargeValueRef *value, jz_bytes bytes, groove_PreparedLargeValue *out) {
    groove_TailOutcome outcome;
    JZ_TRY(groove__large_values__append_tail(value, bytes, &outcome));
    if (outcome.tag == GROOVE_TAIL_UPDATED) { prepared_from_tail(outcome, out); return JZ_OK; }
    return groove__db__facade__Database__consolidate_large_value(db, outcome.value, out);
}

/* Apply a bounded append and persist any consolidation nodes in Groove. */
jz_err groove__db__facade__Database__append_and_stage_large_value(const groove_Database *db, const groove_LargeValueRef *value, jz_bytes bytes, groove_StagedLargeValue *out) {
    groove_PreparedLargeValue prepared;
    JZ_TRY(groove__db__facade__Database__append_large_value(db, value, bytes, &prepared));
    return stage_derived_large_value_preparation(db, value, prepared, out);
}

typedef struct { const groove_LargeValueRef *value; u64 offset, delete_length; const jz_bytes *insert_bytes; groove_TailOutcome out; } EditAttempt;
static jz_err replace_tail_attempt(void *ctx, groove_EvaluationInputs *inputs) {
    EditAttempt *a = ctx;
    return groove__large_values__replace_tail_attempt(a->value, a->offset, a->delete_length, jz_bytes_from(a->insert_bytes->ptr, a->insert_bytes->len), inputs, &a->out);
}

/* Prepare an arbitrary byte-coordinate splice. Consumes `insert_bytes`. */
jz_err groove__db__facade__Database__edit_large_value(const groove_Database *db, const groove_LargeValueRef *value, u64 offset, u64 delete_length, jz_bytes insert_bytes,
                                                      groove_PreparedLargeValue *out) {
    groove_EvaluationInputs *inputs = groove__ivm__runtime__evaluation_session__EvaluationInputs__default();
    EditAttempt a = { value, offset, delete_length, &insert_bytes, {0} };
    jz_err e = attempt_with_chunks(db, inputs, replace_tail_attempt, &a);
    groove__ivm__runtime__evaluation_session__EvaluationInputs__drop(inputs);
    jz_bytes_drop(&insert_bytes);
    JZ_TRY(e);
    if (a.out.tag == GROOVE_TAIL_UPDATED) { prepared_from_tail(a.out, out); return JZ_OK; }
    return groove__db__facade__Database__consolidate_large_value(db, a.out.value, out);
}

/* Apply a byte-coordinate splice and persist any emitted nodes in Groove. */
jz_err groove__db__facade__Database__edit_and_stage_large_value(const groove_Database *db, const groove_LargeValueRef *value, u64 offset, u64 delete_length, jz_bytes insert_bytes,
                                                                groove_StagedLargeValue *out) {
    groove_PreparedLargeValue prepared;
    JZ_TRY(groove__db__facade__Database__edit_large_value(db, value, offset, delete_length, insert_bytes, &prepared));
    return stage_derived_large_value_preparation(db, value, prepared, out);
}

/* ---- metadata listings ---- */

/* Return restart-persistent incomplete push uploads for host expiry policy. */
jz_err groove__db__facade__Database__pending_large_value_uploads(const groove_Database *db, groove_PendingLargeValueUploadVec *out) {
    groove_StorageCursor cursor;
    JZ_TRY(groove__storage__OrderedKvStorage__scan(db_storage(db), groove__storage__ScanRequest__prefix(groove__db__metadata_cf(), jz_bytes_from((const u8 *)"upload/", 7)), &cursor));
    groove_PendingLargeValueUploadVec uploads = {0};
    jz_err e;
    for (;;) {
        bool some;
        groove_KvPairVec batch = {0};
        if ((e = groove__storage__StorageCursor__next_batch(&cursor, &some, &batch)).code || !some) break;
        JZ_VEC_FOR(const groove_KvPair, kv, batch) {
            groove_PendingLargeValueUpload *slot = jz_vec_push_slot(&uploads, sizeof *slot);
            if ((e = groove__db__decode_pending_large_value_upload_at_key(kv->key.ptr, kv->key.len, kv->value.ptr, kv->value.len, slot)).code) { uploads.len--; break; }
        }
        kv_batch_drop(&batch);
        if (e.code) break;
    }
    groove__storage__StorageCursor__drop(&cursor);
    if (e.code) {
        for (u32 i = 0; i < uploads.len; i++) groove__large_values__PendingLargeValueUpload__drop(&uploads.ptr[i]);
        jz_vec_drop(&uploads, sizeof *uploads.ptr, NULL);
        return e;
    }
    *out = uploads;
    return JZ_OK;
}

/* Return persisted opaque staging receipts for host rate/expiry policy. */
jz_err groove__db__facade__Database__staged_large_values(const groove_Database *db, groove_StagedLargeValueVec *out) {
    groove_StorageCursor cursor;
    JZ_TRY(groove__storage__OrderedKvStorage__scan(db_storage(db), groove__storage__ScanRequest__prefix(groove__db__metadata_cf(), jz_bytes_from((const u8 *)"staged/", 7)), &cursor));
    groove_StagedLargeValueVec staged = {0};
    jz_err e;
    for (;;) {
        bool some;
        groove_KvPairVec batch = {0};
        if ((e = groove__storage__StorageCursor__next_batch(&cursor, &some, &batch)).code || !some) break;
        JZ_VEC_FOR(const groove_KvPair, kv, batch) {
            groove_StagedLargeValue *slot = jz_vec_push_slot(&staged, sizeof *slot);
            if ((e = groove__db__decode_staged_large_value_at_key(kv->key.ptr, kv->key.len, kv->value.ptr, kv->value.len, slot)).code) { staged.len--; break; }
        }
        kv_batch_drop(&batch);
        if (e.code) break;
    }
    groove__storage__StorageCursor__drop(&cursor);
    if (e.code) {
        for (u32 i = 0; i < staged.len; i++) groove__large_values__StagedLargeValue__drop(&staged.ptr[i]);
        jz_vec_drop(&staged, sizeof *staged.ptr, NULL);
        return e;
    }
    *out = staged;
    return JZ_OK;
}

static jz_err evict_staged_locked(const groove_Database *db, groove_StagedLargeValueId id, jz_bytes staged_key, bool *out) {
    groove_DynStorage storage = db_storage(db);
    bool found;
    jz_bytes encoded = {0};
    jz_err e = meta_get(storage, &staged_key, &found, &encoded);
    if (e.code || !found) { jz_bytes_drop(&staged_key); *out = false; return e; }
    groove_StagedLargeValue staged;
    e = groove__db__decode_staged_large_value_at_key(staged_key.ptr, staged_key.len, encoded.ptr, encoded.len, &staged);
    jz_bytes_drop(&encoded);
    if (e.code) { jz_bytes_drop(&staged_key); return e; }
    jz_bytes root_key = {0};
    groove_WriteOpVec operations = {0};
    if ((e = groove__db__large_value_root_key(&staged.value_ref.root, &root_key)).code) goto done;
    if ((e = meta_get(storage, &root_key, &found, &encoded)).code) goto done;
    if (!found) { e = LV_ERR("staged root count is missing"); goto done; }
    groove_LargeValueRootReferences references;
    e = groove__db__decode_large_value_root_references(encoded.ptr, encoded.len, &references);
    jz_bytes_drop(&encoded);
    if (e.code) goto done;
    if (references.staged == 0) { e = LV_ERR("staged root count underflow"); goto done; }
    references.staged -= 1;
    bool deactivate_root = references.staged == 0 && references.durable == 0 && references.node_active;
    if (deactivate_root) references.node_active = false;
    jz_bytes root_value;
    if ((e = groove__db__encode_large_value_root_references(&references, &root_value)).code) goto done;
    push_op(&operations, groove__db__metadata_delete(staged_key));
    staged_key = (jz_bytes){0};
    push_op(&operations, groove__db__metadata_set(root_key, root_value));
    root_key = (jz_bytes){0};
    if (deactivate_root) {
        jz_btree node_updates;
        jz_btree_init(&node_updates, &groove__db__NODE_UPDATES_DESC);
        groove_NodeTransitionVec pending = {0};
        JZ_VEC_PUSH(pending, ((groove_NodeTransition){ staged.value_ref.root, -1 }));
        groove_WriteOpVec more;
        if ((e = groove__db__large_value_node_transition_operations(storage, node_updates, pending, false, &more)).code) goto done;
        extend_ops(&operations, &more);
    }
    groove_WriteOpVec cleanup;
    if ((e = groove__db__facade__completed_large_value_cleanup_operations(storage, id, &cleanup)).code) goto done;
    extend_ops(&operations, &cleanup);
    e = groove__storage__OrderedKvStorage__write_many(storage, operations);
    operations = (groove_WriteOpVec){0};
    if (!e.code) *out = true;
done:
    groove__db__WriteOpVec__drop(&operations);
    jz_bytes_drop(&root_key);
    jz_bytes_drop(&staged_key);
    groove__large_values__StagedLargeValue__drop(&staged);
    return e;
}

/* Idempotently evict one unaccepted staging root. Returns false without mutation
 * when absent or while a resident large-value publication owns lifecycle serialization. */
jz_err groove__db__facade__Database__evict_staged_large_value(const groove_Database *db, groove_StagedLargeValueId id, bool *out) {
    *out = false;
    jz_bytes staged_key = groove__db__staged_large_value_key(id);
    /* Waiting for a lock this database holds for resident publications would
     * prevent the caller from reaching persistence; defer instead. */
    if (*db->large_value_lifecycle_held) { jz_bytes_drop(&staged_key); return JZ_OK; }
    groove_StagedWriteOverlay *resident = groove__db__facade__Database__resident_storage(db);
    bool found;
    jz_bytes encoded = {0};
    jz_err e = meta_get(groove__storage__StagedWriteOverlay__as_dyn(resident), &staged_key, &found, &encoded);
    groove__storage__StagedWriteOverlay__release(resident);
    jz_bytes_drop(&encoded);
    if (e.code || !found) { jz_bytes_drop(&staged_key); return e; }
    groove__db__LifecycleMutex__lock(db->large_value_lifecycle);
    e = evict_staged_locked(db, id, staged_key, out);
    groove__db__LifecycleMutex__unlock(db->large_value_lifecycle);
    return e;
}

/* One reclaim-queue entry. *stop when `limit` is reached. */
static jz_err reclaim_entry(const groove_Database *db, const groove_StagedWriteOverlay *resident, const groove_KvPair *entry, u32 *reclaimed) {
    groove_DynStorage storage = db_storage(db);
    groove_NodeRef node_ref_from_key, node_ref_from_value;
    JZ_TRY(groove__db__large_value_reclaim_node_ref_from_key(entry->key.ptr, entry->key.len, &node_ref_from_key));
    jz_err inner = groove__large_values__decode_node_ref(entry->value.ptr, entry->value.len, &node_ref_from_value);
    if (inner.code) {
        jz_err e = groove__db__Error__invalid_large_value_metadata("cannot decode reclaim entry: %.*s", GROOVE_DB_E(inner));
        jz_err_drop(&inner);
        return e;
    }
    if (!node_ref_eq(&node_ref_from_key, &node_ref_from_value)) return LV_ERR("reclaim entry key and value identify different nodes");
    groove_NodeRef node_ref = node_ref_from_key;
    jz_bytes node_key;
    JZ_TRY(groove__db__large_value_node_key(&node_ref, &node_key));
    jz_err e = JZ_OK;
    bool has_override = false;
    groove_LargeValueNodeReferences resident_override, durable_metadata;
    jz_slice staged_value;
    jz_str cf = groove__db__metadata_cf();
    u8 point = groove__storage__StagedWriteOverlay__staged_point_value(resident, cf.ptr, cf.len, node_key.ptr, node_key.len, &staged_value);
    jz_str_drop(&cf);
    if (point == GROOVE_STAGED_POINT_DELETE) { jz_bytes_drop(&node_key); return JZ_OK; }
    if (point == GROOVE_STAGED_POINT_SET) {
        if ((e = groove__db__decode_large_value_node_references(staged_value.ptr, staged_value.len, &resident_override)).code) { jz_bytes_drop(&node_key); return e; }
        has_override = true;
    }
    bool found;
    jz_bytes encoded = {0};
    if ((e = meta_get(storage, &node_key, &found, &encoded)).code || !found) goto done;
    e = groove__db__decode_large_value_node_references(encoded.ptr, encoded.len, &durable_metadata);
    jz_bytes_drop(&encoded);
    if (e.code) goto done;
    u8 decision = groove__db__large_value_reclaim_decision(&durable_metadata, has_override ? &resident_override : &durable_metadata);
    groove__db__LargeValueNodeReferences__drop(&durable_metadata);
    if (decision == GROOVE_RECLAIM_CLEAR_STALE_QUEUE) {
        e = groove__storage__OrderedKvStorage__delete(storage, groove__db__metadata_cf(), jz_bytes_from(entry->key.ptr, entry->key.len));
        goto done;
    }
    if (decision == GROOVE_RECLAIM_WAIT_FOR_DURABLE_ZERO || decision == GROOVE_RECLAIM_RESIDENT_VETO) goto done;
    if ((e = groove__chunks__ChunkStorage__delete(db->chunk_storage, node_ref.locator, node_ref.object_hash)).code) {
        e = groove__chunks__ChunkError__From__from(e);
        goto done;
    }
    groove_WriteOpVec ops = {0};
    push_op(&ops, groove__db__metadata_delete(jz_bytes_from(entry->key.ptr, entry->key.len)));
    push_op(&ops, groove__db__metadata_delete(node_key));
    node_key = (jz_bytes){0};
    if (!(e = groove__storage__OrderedKvStorage__write_many(storage, ops)).code) *reclaimed += 1;
done:
    if (has_override) groove__db__LargeValueNodeReferences__drop(&resident_override);
    jz_bytes_drop(&node_key);
    return e;
}

static jz_err reclaim_with_lifecycle(const groove_Database *db, u32 limit, u32 *out) {
    *out = 0;
    /* Treat the whole provider request/lease population as one coarse ephemeral retainer. */
    groove_OwnedChunkProvider provider = groove__ivm__runtime__IvmRuntime__chunk_provider(db->ivm_runtime);
    void *reclamation_guard = groove__chunks__OwnedChunkProvider__try_begin_reclamation(&provider);
    if (!reclamation_guard) { groove__chunks__OwnedChunkProvider__drop(&provider); return JZ_OK; }
    groove_StagedWriteOverlay *resident = groove__db__facade__Database__resident_storage(db);
    groove_StorageCursor scan;
    jz_err e = groove__storage__OrderedKvStorage__scan(db_storage(db), groove__storage__ScanRequest__prefix(groove__db__metadata_cf(), jz_bytes_from((const u8 *)"reclaim/", 8)), &scan);
    if (!e.code) {
        u32 reclaimed = 0;
        bool stop = false;
        while (!stop) {
            bool some;
            groove_KvPairVec batch = {0};
            if ((e = groove__storage__StorageCursor__next_batch(&scan, &some, &batch)).code || !some) break;
            for (u32 i = 0; i < batch.len; i++) {
                if (reclaimed >= limit) { stop = true; break; }
                if ((e = reclaim_entry(db, resident, &batch.ptr[i], &reclaimed)).code) { stop = true; break; }
            }
            kv_batch_drop(&batch);
        }
        groove__storage__StorageCursor__drop(&scan);
        *out = reclaimed;
    }
    groove__storage__StagedWriteOverlay__release(resident);
    groove__chunks__ChunkReclamationGuard__drop(reclamation_guard);
    groove__chunks__OwnedChunkProvider__drop(&provider);
    return e;
}

/* Drain work authorized by durable reference counts. Resident metadata is a veto only. */
jz_err groove__db__facade__Database__reclaim_orphaned_large_value_chunks(const groove_Database *db, u32 limit, u32 *out) {
    bool take_lock = !*db->large_value_lifecycle_held;
    if (take_lock) groove__db__LifecycleMutex__lock(db->large_value_lifecycle);
    jz_err e = reclaim_with_lifecycle(db, limit, out);
    if (take_lock) groove__db__LifecycleMutex__unlock(db->large_value_lifecycle);
    return e;
}

/* ---- reads ---- */

/* Consume the next bounded window of a sequential logical-value cursor. */
jz_err groove__db__facade__Database__read_large_value_cursor_next(const groove_Database *db, groove_LargeValueCursor *cursor, bool *some, jz_bytes *out) {
    u64 start, end;
    *some = false;
    if (!groove__large_values__LargeValueCursor__next_range(cursor, &start, &end)) return JZ_OK;
    JZ_TRY(groove__db__facade__Database__read_large_value_range(db, groove__large_values__LargeValueCursor__value(cursor), start, end, out));
    groove__large_values__LargeValueCursor__advance_to(cursor, end);
    *some = true;
    return JZ_OK;
}

/* async fn cooperative_yield_once(): wake self once and return Pending. */
static void cooperative_yield_once(void) { jz_task_run_ready(); }

/* Compute a BLAKE3 checksum without materializing the complete logical value. Consumes `value`. */
jz_err groove__db__facade__Database__checksum_large_value_streaming(const groove_Database *db, groove_LargeValueRef value, u32 window_bytes, u32 max_bytes_per_turn,
                                                                    groove_ContentHash *hash, groove_StreamingExecutionStats *stats) {
    groove_StreamingChecksum *op;
    JZ_TRY(groove__large_values__StreamingChecksum__new(value, window_bytes, max_bytes_per_turn, &op));
    jz_err e = JZ_OK;
    while (groove__large_values__LargeValueCursor__remaining_bytes(groove__large_values__StreamingChecksum__cursor(op)) != 0) {
        u64 start, end;
        if (!groove__large_values__LargeValueCursor__next_range(groove__large_values__StreamingChecksum__cursor(op), &start, &end))
            jz_panic("non-complete cursor has a next range");
        jz_bytes window;
        if ((e = groove__db__facade__Database__read_large_value_range(db, groove__large_values__LargeValueCursor__value(groove__large_values__StreamingChecksum__cursor(op)),
                                                                      start, end, &window)).code)
            break;
        bool yield;
        e = groove__large_values__StreamingChecksum__consume_window(op, window.ptr, window.len, &yield);
        jz_bytes_drop(&window);
        if (e.code) break;
        if (yield) {
            cooperative_yield_once();
            if ((e = groove__large_values__StreamingChecksum__record_yield(op)).code) break;
        }
    }
    if (!e.code) e = groove__large_values__StreamingChecksum__finish(op, hash, stats);
    groove__large_values__StreamingChecksum__drop(op);
    return e;
}

typedef struct { const groove_LargeValueRef *value; u64 start, end, offset; jz_bytes bytes; u64 byte; } Utf16Attempt;
static jz_err utf16_range_attempt(void *ctx, groove_EvaluationInputs *inputs) {
    Utf16Attempt *a = ctx;
    return groove__large_values__utf16_range_attempt(a->value, a->start, a->end, inputs, &a->bytes);
}
static jz_err utf16_offset_attempt(void *ctx, groove_EvaluationInputs *inputs) {
    Utf16Attempt *a = ctx;
    return groove__large_values__utf16_offset_to_byte_attempt(a->value, a->offset, inputs, &a->byte);
}

/* Read a UTF-16 code-unit range from a large string or JSON value. */
jz_err groove__db__facade__Database__read_large_text_utf16_range(const groove_Database *db, const groove_LargeValueRef *value, u64 start, u64 end, jz_str *out) {
    groove_EvaluationInputs *inputs = groove__ivm__runtime__evaluation_session__EvaluationInputs__default();
    Utf16Attempt a = { value, start, end, 0, {0}, 0 };
    jz_err e = attempt_with_chunks(db, inputs, utf16_range_attempt, &a);
    groove__ivm__runtime__evaluation_session__EvaluationInputs__drop(inputs);
    JZ_TRY(e);
    if (!jz_utf8_valid(a.bytes.ptr, a.bytes.len)) {
        jz_bytes_drop(&a.bytes);
        return groove__large_values__Error__new(GROOVE_EXT_LV_E_INVALID_UTF8);
    }
    *out = (jz_str){ (char *)a.bytes.ptr, a.bytes.len, a.bytes.cap }; /* String::from_utf8 */
    return JZ_OK;
}

/* Resolve a final logical UTF-16 position to its byte position. */
jz_err groove__db__facade__Database__large_text_utf16_offset_to_byte(const groove_Database *db, const groove_LargeValueRef *value, u64 offset, u64 *out) {
    groove_EvaluationInputs *inputs = groove__ivm__runtime__evaluation_session__EvaluationInputs__default();
    Utf16Attempt a = { value, 0, 0, offset, {0}, 0 };
    jz_err e = attempt_with_chunks(db, inputs, utf16_offset_attempt, &a);
    groove__ivm__runtime__evaluation_session__EvaluationInputs__drop(inputs);
    JZ_TRY(e);
    *out = a.byte;
    return JZ_OK;
}

/* serde_json::Value::pointer (RFC 6901). Returns a borrowed node or NULL. */
static const jz_json *json_pointer(const jz_json *root, const char *pointer, u32 n) {
    if (n == 0) return root;
    if (pointer[0] != '/') return NULL;
    const jz_json *target = root;
    u32 i = 1;
    for (;;) {
        u32 j = i;
        while (j < n && pointer[j] != '/') j++;
        jz_str token = {0};
        for (u32 k = i; k < j; k++) {
            if (pointer[k] == '~' && k + 1 < j && pointer[k + 1] == '1') { jz_str_push_char(&token, '/'); k++; }
            else if (pointer[k] == '~' && k + 1 < j && pointer[k + 1] == '0') { jz_str_push_char(&token, '~'); k++; }
            else jz_str_push_char(&token, pointer[k]);
        }
        const jz_json *next = NULL;
        if (target->kind == JZ_JSON_OBJ) {
            jz_json **slot = jz_btree_get(&target->v.obj, &token);
            next = slot ? *slot : NULL;
        } else if (target->kind == JZ_JSON_ARR) {
            /* parse_index: no leading '+', no leading zeros */
            bool ok = token.len > 0 && !(token.len > 1 && token.ptr[0] == '0');
            u64 index = 0;
            for (u32 k = 0; ok && k < token.len; k++) {
                if (token.ptr[k] < '0' || token.ptr[k] > '9') ok = false;
                else index = index * 10 + (u64)(token.ptr[k] - '0');
            }
            if (ok && index < target->v.arr.len) next = target->v.arr.ptr[index];
        }
        jz_str_drop(&token);
        if (!next) return NULL;
        target = next;
        if (j >= n) return target;
        i = j + 1;
    }
}

/* Resolve a JSON Pointer against literal validated JSON source. *out NULL = None. */
jz_err groove__db__facade__Database__read_large_json_pointer(const groove_Database *db, const groove_LargeValueRef *value, const char *pointer, u32 pn, jz_json **out) {
    *out = NULL;
    if (value->kind != GROOVE_LVK_JSON) return groove__large_values__Error__new(GROOVE_EXT_LV_E_INVALID_JSON);
    /* JSON validity is a write-admission invariant; parse only the demanded source. */
    jz_bytes source = {0};
    jz_err e = JZ_OK;
    u64 offset = 0;
    while (offset < value->byte_length) {
        u64 end = offset + GROOVE_EXT_LV_LEAF_MIN_BYTES;
        if (end < offset || end > value->byte_length) end = value->byte_length;
        jz_bytes chunk;
        if ((e = groove__db__facade__Database__read_large_value_range(db, value, offset, end, &chunk)).code) goto done;
        jz_vec_extend(&source, chunk.ptr, chunk.len, 1);
        jz_bytes_drop(&chunk);
        groove_JsonPointerPrefix prefix;
        if ((e = groove__large_values__json_pointer_prefix(source.ptr, source.len, pointer, pn, &prefix)).code) goto done;
        if (prefix.tag == GROOVE_JSON_PTR_FOUND) { *out = prefix.found; goto done; }
        if (prefix.tag == GROOVE_JSON_PTR_NEED_MORE) { offset = end; continue; }
        /* RequiresFullDocument */
        if (end < value->byte_length) {
            if ((e = groove__db__facade__Database__read_large_value_range(db, value, end, value->byte_length, &chunk)).code) goto done;
            jz_vec_extend(&source, chunk.ptr, chunk.len, 1);
            jz_bytes_drop(&chunk);
        }
        break;
    }
    jz_json *json;
    jz_err parse = jz_json_parse((jz_slice){ source.ptr, source.len }, &json);
    if (parse.code) { jz_err_drop(&parse); e = groove__large_values__Error__new(GROOVE_EXT_LV_E_INVALID_JSON); goto done; }
    const jz_json *found = json_pointer(json, pointer, pn);
    *out = found ? jz_json_clone(found) : NULL;
    jz_json_free(json);
done:
    jz_bytes_drop(&source);
    return e;
}

/* ======================= impl Database (runtime handles) ======================= */

/* Include arrangement and recursive-state size walks in future tick metrics. */
void groove__db__facade__Database__set_tick_runtime_stats_enabled(groove_Database *db, bool enabled) {
    groove__ivm__runtime__IvmRuntime__set_tick_runtime_stats_enabled(db->ivm_runtime, enabled);
}
/* Compute full runtime stats on demand. */
groove_RuntimeStats *groove__db__facade__Database__runtime_stats(const groove_Database *db) { return groove__ivm__runtime__IvmRuntime__stats(db->ivm_runtime); }
/* Lifetime topology-cache counters. */
groove_ExecutionLayoutStats groove__db__facade__Database__execution_layout_stats(const groove_Database *db) {
    return groove__ivm__runtime__IvmRuntime__execution_layout_stats(db->ivm_runtime);
}

groove_RecordStore groove__db__facade__Database__durable_indices_store_with_storage(const groove_Database *db, groove_DynStorage storage, groove_RecordDescriptor descriptor) {
    (void)db;
    return groove__storage__RecordStore__new(storage, "indices", 7, descriptor);
}

void groove__db__facade__Database__open_batch(const groove_Database *db, groove_DatabaseBatch *out) {
    (void)db;
    groove__db__batch__DatabaseBatch__default(out);
}

/* Whether any binding currently holds the prepared binding source named `shape`. */
bool groove__db__facade__Database__prepared_binding_source_is_bound(const groove_Database *db, const char *shape, u32 n) {
    return groove__ivm__runtime__IvmRuntime__prepared_binding_source_is_bound(db->ivm_runtime, shape, n);
}

/* Return a typed handle for a schema-declared direct record store. */
jz_err groove__db__facade__Database__direct_record_store(const groove_Database *db, const char *name, u32 n, groove_DirectRecordStore *out) {
    const groove_DirectRecordStoreSchema *schema;
    JZ_TRY(groove__db__primary_storage__Database__direct_record_store_schema(db, name, n, &schema));
    out->storage = db->storage;
    out->name = jz_str_clone(&schema->name);
    out->key = groove__schema__DirectRecordStoreSchema__key_descriptor(schema);
    out->value = groove__schema__DirectRecordStoreSchema__value_descriptor(schema);
    return JZ_OK;
}

/* Write a direct metadata batch while advancing retained query work that may
 * itself own the storage writer. Rust polls `poll_progress` (through a wake
 * bridge of the operation and `progress_waker`) only while the write future is
 * Pending; the synchronous storage write is Ready on its first poll, so that
 * branch is never entered and `progress_waker` is unused. */
jz_err groove__db__facade__Database__write_direct_records_with_progress(groove_Database *db, const char *name, u32 n, const groove_DirectRecordStoreWrite *operations, u32 nops,
                                                                        const jz_waker *progress_waker) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    groove_DirectRecordStore store;
    JZ_TRY(groove__db__facade__Database__direct_record_store(db, name, n, &store));
    groove_WriteOpVec encoded;
    jz_err e = groove__db__DirectRecordStore__encode_writes(&store, operations, nops, &encoded);
    jz_str_drop(&store.name);
    JZ_TRY(e);
    groove_LayoutStorage *storage = jz_rc_clone(db->storage);
    (void)progress_waker;
    e = groove__storage__OrderedKvStorage__write_many(groove__storage__LayoutStorage__as_dyn(storage), encoded);
    jz_rc_release(storage);
    return e;
}
