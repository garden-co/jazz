/* db/commit.c: port of crates/groove/src/db/commit.rs.
 * Batch application (resident publication), ordered persistence settlement,
 * pending-write preparation and descriptor lookup.
 * (commit_batch and pending_writes_from_operations are test-only.) */
#include "mod.h"

static void noop_wake(void *ctx) { (void)ctx; }
static const jz_waker NOOP_WAKER = { noop_wake, NULL };
static void str_drop_v(void *p) { jz_str_drop(p); }
static int str_cmp2(const void *a, const void *b) { return jz_str_cmp(a, b); }

/* Run one IVM tick without base-table writes. */
jz_err groove__db__commit__Database__flush(groove_Database *db) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    jz_err e = groove__ivm__runtime__IvmRuntime__drive_pending_incremental(db->ivm_runtime);
    if (e.code) { db->poisoned = true; return e; }
    groove__db__commit__Database__refresh_resident_writes(db);
    groove_StagedWriteOverlay *resident = groove__storage__StagedWriteOverlay__new(groove__storage__LayoutStorage__as_dyn(db->storage), db->resident_writes);
    groove_MeteredStorage storage = groove__db__MeteredStorage__new(groove__storage__StagedWriteOverlay__as_dyn(resident), db->storage_read_metrics);
    groove_TickMetrics *tick;
    e = groove__ivm__runtime__IvmRuntime__tick(db->ivm_runtime, NULL, 0, groove__db__MeteredStorage__as_dyn(&storage), &tick);
    groove__db__MeteredStorage__drop(&storage);
    groove__storage__StagedWriteOverlay__release(resident);
    if (e.code) { db->poisoned = true; return e; }
    if (db->last_tick_metrics) groove__ivm__runtime__TickMetrics__drop(db->last_tick_metrics);
    db->last_tick_metrics = tick;
    return JZ_OK;
}
bool groove__db__commit__Database__last_commit_metrics(const groove_Database *db, const groove_CommitMetrics **out) {
    if (!db->has_last_commit_metrics) return false;
    *out = &db->last_commit_metrics;
    return true;
}
const groove_TickMetrics *groove__db__commit__Database__last_tick_metrics(const groove_Database *db) { return db->last_tick_metrics; }
groove_StorageReadMetrics groove__db__commit__Database__storage_read_metrics(const groove_Database *db) { return db->storage_read_metrics->metrics; }
void groove__db__commit__Database__reset_storage_read_metrics(const groove_Database *db) { memset(&db->storage_read_metrics->metrics, 0, sizeof db->storage_read_metrics->metrics); }
groove_StorageReadMetrics groove__db__commit__Database__take_storage_read_metrics(const groove_Database *db) {
    groove_StorageReadMetrics metrics = groove__db__commit__Database__storage_read_metrics(db);
    groove__db__commit__Database__reset_storage_read_metrics(db);
    return metrics;
}

/* ---- visitors for visit_large_value_refs ---- */
typedef struct { const groove_LargeValueRef *target; } RefEqCtx;
static bool visit_ref_eq(void *ctx, const groove_LargeValueRef *reference) {
    return groove__large_values__LargeValueRef__PartialEq__eq(reference, ((RefEqCtx *)ctx)->target);
}
typedef struct { jz_btree *deltas; i64 weight; } RootDeltaCtx;
static bool visit_root_delta(void *ctx, const groove_LargeValueRef *reference) {
    RootDeltaCtx *c = ctx;
    bool inserted;
    i64 *slot = jz_btree_entry(c->deltas, &reference->root, &inserted);
    if (inserted) *slot = 0;
    *slot += c->weight;
    return false;
}

static void layout_storage_cell_drop(void *p) { groove_LayoutStorageCell *c = p; if (c->storage) jz_rc_release(c->storage); }
static groove_LayoutStorageCell *layout_storage_cell_new(groove_LayoutStorage *storage) {
    groove_LayoutStorageCell *c = jz_rc_new(sizeof *c, layout_storage_cell_drop);
    c->storage = storage;
    return c;
}
static jz_err resident_get(const groove_Database *db, jz_bytes key, bool *found, jz_bytes *out) {
    groove_StagedWriteOverlay *resident = groove__db__facade__Database__resident_storage(db);
    jz_err e = groove__storage__OrderedKvStorage__get(groove__storage__StagedWriteOverlay__as_dyn(resident), groove__db__metadata_cf(), key, found, out);
    groove__storage__StagedWriteOverlay__release(resident);
    return e;
}
static void lifecycle_guard_drop(groove_LifecycleMutex *guard) {
    if (!guard) return;
    groove__db__LifecycleMutex__unlock(guard);
    jz_rc_release(guard);
}

/* Take the large-value lifecycle mutex for a publication's root transitions.
 * While the mutex is contended, keep driving suspended evaluations until their
 * installs let it go. */
static jz_err lock_large_value_lifecycle(groove_Database *db, groove_LifecycleMutex **out) {
    for (;;) {
        if (groove__db__LifecycleMutex__try_lock(db->large_value_lifecycle)) {
            *out = jz_rc_clone(db->large_value_lifecycle);
            return JZ_OK;
        }
        jz_err e;
        if (groove__db__query__Database__poll_progress(db, &NOOP_WAKER, &e) && e.code) return e;
        /* An install that finished just now released the mutex. */
        if (groove__db__LifecycleMutex__try_lock(db->large_value_lifecycle)) {
            *out = jz_rc_clone(db->large_value_lifecycle);
            return JZ_OK;
        }
        if (!groove__db__query__Database__has_pending_progress(db)) jz_panic("large-value lifecycle mutex cannot make progress");
    }
}

/* Apply resident rows and unblocked terminal deltas before ordered persistence. */
jz_err groove__db__commit__Database__apply_batch(groove_Database *db, groove_DatabaseBatch batch, groove_AppliedBatch *out) {
    jz_err e = groove__db__batch__DatabaseBatch__check_exact_base(&batch, db);
    if (!e.code) e = groove__db__commit__Database__ensure_not_poisoned(db);
    if (e.code) { groove__db__batch__DatabaseBatch__drop(&batch); return e; }
    groove_StagedLargeValueIdVec accepted_large_values = {0};
    jz_vec_extend(&accepted_large_values, batch.accepted_large_values.ptr, batch.accepted_large_values.len, sizeof(groove_StagedLargeValueId));
    bool defer_notifications_until_durable = batch.notification_timing == GROOVE_NOTIFY_AFTER_PERSISTENCE;
    /* Later ordinary writes must not invalidate an ensure_exact result. */
    jz_btree exact_keys = batch.exact_keys;
    jz_btree_init(&batch.exact_keys, &groove__db__EXACT_KEYS_DESC);

    groove_PendingTableWriteVec pending_writes = {0};
    groove_StagedLargeValueVec accepted_staging = {0};
    groove_TableDeltaVec table_deltas = {0};
    bool have_table_deltas = false;
    groove_StrVec changed_tables = {0};
    jz_btree durable_root_deltas, accepted_roots, roots;
    jz_btree_init(&durable_root_deltas, &groove__db__NODE_I64_DESC);
    jz_btree_init(&accepted_roots, &groove__db__NODE_U64_DESC);
    jz_btree_init(&roots, &groove__db__NODE_SET_DESC);
    groove_WriteOpVec staged_operations = {0};
    groove_IndexIdRegistrations *index_id_registrations = NULL;
    groove_StagedWriteState *staged_state = NULL;
    groove_StagedWriteOverlay *storage = NULL;
    bool *resident_install_durable = NULL;
    groove_ResidentTick *resident_tick = NULL;
    groove_LifecycleMutex *lifecycle_guard = NULL;

    e = groove__db__commit__Database__pending_writes_from_batch(db, &batch, &pending_writes);
    groove__db__batch__DatabaseBatch__drop(&batch);
    if (e.code) goto fail;
    for (u32 i = 0; i < pending_writes.len; i++) {
        const groove_PendingTableWrite *write = &pending_writes.ptr[i];
        groove_ExactKey probe = { { write->table.ptr, write->table.len, 0 }, { write->key.ptr, write->key.len, 0 } };
        const jz_bytes *expected = jz_btree_get(&exact_keys, &probe);
        if (!expected) continue;
        jz_bytes stored;
        bool has = groove__db__PendingTableWrite__stored_record(write, &stored);
        bool same = has && jz_bytes_eq(&stored, expected);
        if (has) jz_bytes_drop(&stored);
        if (!same) { e = groove__db__Error__unit(GROOVE_DB_ImmutableBatchConflict); goto fail; }
    }
    for (u32 i = 0; i < accepted_large_values.len; i++) {
        jz_bytes key = groove__db__staged_large_value_key(accepted_large_values.ptr[i]);
        /* Read through resident writes: an earlier batch whose persistence is
         * still pending may already have consumed this id. */
        bool found;
        jz_bytes encoded;
        e = resident_get(db, jz_bytes_clone(&key), &found, &encoded);
        if (!e.code && !found) e = groove__db__Error__invalid_large_value_metadata("accepted staging id is missing or already consumed");
        if (e.code) { jz_bytes_drop(&key); goto fail; }
        groove_StagedLargeValue staged;
        e = groove__db__decode_staged_large_value_at_key(key.ptr, key.len, encoded.ptr, encoded.len, &staged);
        jz_bytes_drop(&key);
        jz_bytes_drop(&encoded);
        if (e.code) goto fail;
        JZ_VEC_PUSH(accepted_staging, staged);
    }
    for (u32 s = 0; s < accepted_staging.len; s++) {
        bool found = false;
        RefEqCtx ctx = { &accepted_staging.ptr[s].value_ref };
        for (u32 i = 0; i < pending_writes.len && !found; i++) {
            const groove_PendingTableWrite *write = &pending_writes.ptr[i];
            if (write->tag != GROOVE_PENDING_SET) continue;
            if ((e = groove__records__RecordDescriptor__visit_large_value_refs(write->descriptor, write->record.ptr, write->record.len, visit_ref_eq, &ctx, &found)).code) goto fail;
        }
        if (!found) {
            e = groove__db__Error__invalid_large_value_metadata("accepted staging root is not referenced by this physical-record batch");
            goto fail;
        }
    }
    {
        groove_StagedWriteOverlay *overlay = groove__storage__StagedWriteOverlay__new(groove__storage__LayoutStorage__as_dyn(db->storage), db->resident_writes);
        groove_DynStorage overlay_dyn = groove__storage__StagedWriteOverlay__as_dyn(overlay);
        groove_RecordStore *stores = jz_alloc((pending_writes.len ? pending_writes.len : 1) * sizeof(groove_RecordStore));
        for (u32 i = 0; i < pending_writes.len; i++)
            stores[i] = groove__storage__RecordStore__new(overlay_dyn, (const char *)pending_writes.ptr[i].table.ptr, pending_writes.ptr[i].table.len, pending_writes.ptr[i].descriptor);
        e = groove__db__compute_table_deltas(pending_writes.ptr, stores, pending_writes.len, groove__ivm__runtime__IvmRuntime__schema(db->ivm_runtime), &table_deltas);
        jz_free(stores);
        groove__storage__StagedWriteOverlay__release(overlay);
        if (e.code) goto fail;
        have_table_deltas = true;
    }
    for (u32 i = 0; i < table_deltas.len; i++) JZ_VEC_PUSH(changed_tables, jz_str_clone(&table_deltas.ptr[i].table));
    jz_sort_unstable(changed_tables.ptr, changed_tables.len, sizeof(jz_str), jz_cmp_str, NULL);
    jz_vec_dedup(&changed_tables, sizeof(jz_str), str_cmp2, str_drop_v);
    for (u32 t = 0; t < table_deltas.len; t++) {
        const groove_TableDelta *table_delta = &table_deltas.ptr[t];
        for (u32 d = 0; d < table_delta->deltas.len; d++) {
            RootDeltaCtx ctx = { &durable_root_deltas, table_delta->deltas.ptr[d].weight };
            bool stop;
            if ((e = groove__records__RecordDescriptor__visit_large_value_refs(table_delta->descriptor, table_delta->deltas.ptr[d].record.ptr,
                                                                               table_delta->deltas.ptr[d].record.len, visit_root_delta, &ctx, &stop)).code) goto fail;
        }
    }
    for (u32 i = 0; i < pending_writes.len; i++) {
        groove_PendingTableWrite *write = &pending_writes.ptr[i];
        groove_OwnedWriteOperation op;
        if (write->tag == GROOVE_PENDING_SET) {
            jz_bytes value;
            groove__records__encode_variant_record(write->variant_tag, write->record.ptr, write->record.len, &value);
            op.tag = GROOVE_WRITE_OP_SET; op.cf = write->table; op.key = write->key; op.value = value;
            jz_bytes_drop(&write->record);
        } else {
            op.tag = GROOVE_WRITE_OP_DELETE; op.cf = write->table; op.key = write->key; memset(&op.value, 0, sizeof op.value);
        }
        JZ_VEC_PUSH(staged_operations, op);
    }
    jz_vec_drop(&pending_writes, sizeof(groove_PendingTableWrite), NULL);
    for (u32 i = 0; i < accepted_large_values.len; i++) {
        groove_StagedLargeValueId staged_id = accepted_large_values.ptr[i];
        jz_bytes key = groove__db__staged_large_value_key(staged_id);
        bool found;
        jz_bytes encoded;
        e = resident_get(db, jz_bytes_clone(&key), &found, &encoded);
        if (e.code) { jz_bytes_drop(&key); goto fail; }
        if (!found) {
            jz_bytes_drop(&key);
            e = groove__db__Error__invalid_large_value_metadata("accepted staging id is missing or already consumed");
            goto fail;
        }
        jz_bytes_drop(&encoded);
        JZ_VEC_PUSH(staged_operations, groove__db__metadata_delete(key));
        groove_StagedWriteOverlay *resident = groove__db__facade__Database__resident_storage(db);
        groove_WriteOpVec cleanup;
        e = groove__db__facade__completed_large_value_cleanup_operations(groove__storage__StagedWriteOverlay__as_dyn(resident), staged_id, &cleanup);
        groove__storage__StagedWriteOverlay__release(resident);
        if (e.code) goto fail;
        jz_vec_extend(&staged_operations, cleanup.ptr, cleanup.len, sizeof(groove_OwnedWriteOperation));
        jz_vec_drop(&cleanup, sizeof(groove_OwnedWriteOperation), NULL);
    }
    for (u32 s = 0; s < accepted_staging.len; s++) {
        bool inserted;
        u64 *count = jz_btree_entry(&accepted_roots, &accepted_staging.ptr[s].value_ref.root, &inserted);
        if (inserted) *count = 0;
        *count += 1;
    }
    {
        groove_NodeRef *k; void *v;
        JZ_BTREE_FOR(&durable_root_deltas, k, v) jz_btree_insert(&roots, k, NULL, NULL);
        JZ_BTREE_FOR(&accepted_roots, k, v) jz_btree_insert(&roots, k, NULL, NULL);
    }
    /* Index ids allocated since the last durable write become durable
     * atomically with this publication. */
    index_id_registrations = groove__ivm__runtime__IndexIdRegistry__pending(groove__ivm__runtime__IvmRuntime__index_ids(db->ivm_runtime));
    {
        u32 n;
        const groove_OwnedWriteOperation *ops = groove__ivm__runtime__IndexIdRegistrations__operations(index_id_registrations, &n);
        for (u32 i = 0; i < n; i++) {
            groove_OwnedWriteOperation copy;
            groove__storage__OwnedWriteOperation__Clone__clone(&copy, &ops[i]);
            JZ_VEC_PUSH(staged_operations, copy);
        }
    }
    {
        groove_StagedWriteOverlay *resident_overlay = groove__storage__StagedWriteOverlay__new_owned(
            jz_rc_clone(db->storage), groove__storage__LayoutStorage__as_dyn(db->storage), jz_rc_clone(db->resident_writes));
        staged_state = groove__storage__StagedWriteState__from_ops(staged_operations);
        memset(&staged_operations, 0, sizeof staged_operations);
        storage = groove__storage__StagedWriteOverlay__new_owned(resident_overlay, groove__storage__StagedWriteOverlay__as_dyn(resident_overlay), jz_rc_clone(staged_state));
    }
    resident_install_durable = groove__db__cell_bool_new(false);
    {
        groove_PublicationInstallFailures resident_install_failures = groove__chunks__PublicationInstallFailures__default();
        groove_ResidentLifecycleInstall install = {
            groove__storage__OwnedStorage__new(jz_rc_clone(storage), groove__storage__StagedWriteOverlay__as_dyn(storage)),
            staged_state,
            db->large_value_lifecycle_held,
            resident_install_durable,
            resident_install_failures,
        };
        groove_DynChunkInstallObserver resident_install_observer = groove__db__MetadataChunkInstallObserver__new_dyn(db->storage, db->large_value_lifecycle, &install);
        groove__storage__OwnedStorage__drop(&install.storage);
        u64 tick_start = jz_now_ms();
        have_table_deltas = false; /* moved */
        e = groove__ivm__runtime__IvmRuntime__tick_resident_staged(db->ivm_runtime, &table_deltas,
                                                                   groove__storage__OwnedStorage__new(jz_rc_clone(storage), groove__storage__StagedWriteOverlay__as_dyn(storage)),
                                                                   defer_notifications_until_durable, true, resident_install_observer, resident_install_failures, &resident_tick);
        if (e.code) { db->poisoned = true; goto fail; }
        /* Durable nodes are already evaluated, even when a query terminal is
         * waiting for cold content. Include their writes in this publication's
         * atomic batch now. */
        groove__storage__StagedWriteState__extend(staged_state, groove__ivm__runtime__ResidentTick__take_durable_writes(resident_tick));
        /* Finish self-scheduled CPU-only continuations before returning the
         * publication so its subscription output is observable in the same
         * direct write turn. */
        if ((e = groove__db__query__Database__drain_self_scheduled_resident_progress(db)).code) goto fail;
        u64 ivm_tick_time = jz_now_ms() - tick_start;
        /* The IVM's resident observer takes the lifecycle lock itself only
         * when another resident publication does not already hold it. */
        if (roots.len != 0 && !db->large_value_publication_lifecycle_guard)
            if ((e = lock_large_value_lifecycle(db, &lifecycle_guard)).code) goto fail;
        if (roots.len != 0) {
            groove_NodeTransitionVec node_transitions = {0};
            groove_WriteOpVec lifecycle_operations = {0};
            groove_DynStorage sd = groove__storage__StagedWriteOverlay__as_dyn(storage);
            groove_NodeRef *root; void *unused;
            JZ_BTREE_FOR(&roots, root, unused) {
                jz_bytes key;
                if ((e = groove__db__large_value_root_key(root, &key)).code) break;
                bool found;
                jz_bytes encoded;
                if ((e = groove__storage__OrderedKvStorage__get(sd, groove__db__metadata_cf(), jz_bytes_clone(&key), &found, &encoded)).code) { jz_bytes_drop(&key); break; }
                groove_LargeValueRootReferences references = {0};
                if (found) {
                    e = groove__db__decode_large_value_root_references(encoded.ptr, encoded.len, &references);
                    jz_bytes_drop(&encoded);
                    if (e.code) { jz_bytes_drop(&key); break; }
                }
                u64 previous_total = references.durable + references.staged;
                if (previous_total < references.durable) previous_total = UINT64_MAX;
                i64 *dd = jz_btree_get(&durable_root_deltas, root);
                i64 durable_delta = dd ? *dd : 0;
                bool ok;
                if (durable_delta >= 0) { ok = references.durable <= UINT64_MAX - (u64)durable_delta; references.durable += ok ? (u64)durable_delta : 0; }
                else { u64 abs = (u64)(-(durable_delta + 1)) + 1; ok = references.durable >= abs; references.durable -= ok ? abs : 0; }
                if (!ok) { jz_bytes_drop(&key); e = groove__db__Error__invalid_large_value_metadata("durable root count overflow/underflow"); break; }
                u64 *ar = jz_btree_get(&accepted_roots, root);
                u64 accepted = ar ? *ar : 0;
                if (references.staged < accepted) { jz_bytes_drop(&key); e = groove__db__Error__invalid_large_value_metadata("staged root count underflow"); break; }
                references.staged -= accepted;
                u64 next_total = references.durable + references.staged;
                if (next_total < references.durable) next_total = UINT64_MAX;
                if (previous_total == 0 && next_total > 0 && !references.node_active) {
                    jz_bytes node_key;
                    if ((e = groove__db__large_value_node_key(root, &node_key)).code) { jz_bytes_drop(&key); break; }
                    jz_bytes node;
                    if ((e = groove__storage__OrderedKvStorage__get(sd, groove__db__metadata_cf(), node_key, &found, &node)).code) { jz_bytes_drop(&key); break; }
                    if (found) {
                        jz_bytes_drop(&node);
                        references.node_active = true;
                        groove_NodeTransition t = { *root, 1 };
                        JZ_VEC_PUSH(node_transitions, t);
                    }
                } else if (previous_total > 0 && next_total == 0 && references.node_active) {
                    references.node_active = false;
                    groove_NodeTransition t = { *root, -1 };
                    JZ_VEC_PUSH(node_transitions, t);
                }
                jz_bytes value;
                if ((e = groove__db__encode_large_value_root_references(&references, &value)).code) { jz_bytes_drop(&key); break; }
                JZ_VEC_PUSH(lifecycle_operations, groove__db__metadata_set(key, value));
            }
            if (!e.code) {
                jz_btree empty;
                jz_btree_init(&empty, &groove__db__NODE_UPDATES_DESC);
                groove_WriteOpVec node_ops;
                e = groove__db__large_value_node_transition_operations(sd, empty, node_transitions, false, &node_ops);
                memset(&node_transitions, 0, sizeof node_transitions);
                if (!e.code) {
                    jz_vec_extend(&lifecycle_operations, node_ops.ptr, node_ops.len, sizeof(groove_OwnedWriteOperation));
                    jz_vec_drop(&node_ops, sizeof(groove_OwnedWriteOperation), NULL);
                }
            }
            jz_vec_drop(&node_transitions, sizeof(groove_NodeTransition), NULL);
            if (e.code) { groove__db__WriteOpVec__drop(&lifecycle_operations); goto fail; }
            groove__storage__StagedWriteState__extend(staged_state, lifecycle_operations);
        }
        /* Every fallible/cancellable operation is complete. Allocate the id,
         * bind buffered notifications, and register the publication. */
        groove_PublicationId publication = db->next_publication_id;
        db->next_publication_id = db->next_publication_id == UINT64_MAX ? UINT64_MAX : db->next_publication_id + 1;
        groove_TickMetrics *tick = groove__ivm__runtime__IvmRuntime__assign_resident_publication(db->ivm_runtime, resident_tick, publication);
        resident_tick = NULL;
        groove__storage__StagedWriteState__extend_shared(db->resident_writes, groove__storage__StagedWriteState__snapshot(staged_state));
        void *state_ref = jz_rc_clone(staged_state);
        jz_btree_insert(&db->resident_publications, &publication, &state_ref, NULL);
        /* A failed persistence of this publication poisons the database, so the
         * registrations it carries count as written from here on. */
        groove__ivm__runtime__IndexIdRegistry__mark_persisted(groove__ivm__runtime__IvmRuntime__index_ids(db->ivm_runtime), index_id_registrations);
        if (roots.len != 0) {
            if (lifecycle_guard) {
                db->large_value_publication_lifecycle_guard = lifecycle_guard;
                lifecycle_guard = NULL;
                *db->large_value_lifecycle_held = true;
            }
            jz_btree_insert(&db->large_value_lifecycle_publications, &publication, NULL, NULL);
        }
        memset(out, 0, sizeof *out);
        out->publication = publication;
        out->storage = layout_storage_cell_new(jz_rc_clone(db->storage));
        out->operations = staged_state;
        staged_state = NULL;
        out->resident_install_durable = resident_install_durable;
        resident_install_durable = NULL;
        out->order = jz_rc_clone(db->publication_persistence);
        out->ivm_tick_time_ms = ivm_tick_time;
        out->tick = tick;
        out->changed_tables = changed_tables;
        memset(&changed_tables, 0, sizeof changed_tables);
        out->notifications_deferred = defer_notifications_until_durable;
        out->lifecycle = jz_rc_new(sizeof(u8), NULL);
        *out->lifecycle = GROOVE_APPLIED_APPLIED;
        out->abandoned_application = jz_rc_clone(db->abandoned_application);
        out->index_ids = jz_rc_clone(groove__ivm__runtime__IvmRuntime__index_ids(db->ivm_runtime));
        e = JZ_OK;
    }
fail:
    lifecycle_guard_drop(lifecycle_guard);
    if (storage) groove__storage__StagedWriteOverlay__release(storage);
    if (staged_state) jz_rc_release(staged_state);
    if (resident_install_durable) jz_rc_release(resident_install_durable);
    if (index_id_registrations) groove__ivm__runtime__IndexIdRegistrations__drop(index_id_registrations);
    groove__db__WriteOpVec__drop(&staged_operations);
    jz_btree_drop(&roots);
    jz_btree_drop(&accepted_roots);
    jz_btree_drop(&durable_root_deltas);
    jz_vec_drop(&changed_tables, sizeof(jz_str), str_drop_v);
    if (have_table_deltas) jz_vec_drop(&table_deltas, sizeof(groove_TableDelta), groove__ivm__runtime__TableDelta__drop);
    for (u32 i = 0; i < accepted_staging.len; i++) groove__large_values__StagedLargeValue__drop(&accepted_staging.ptr[i]);
    jz_vec_drop(&accepted_staging, sizeof(groove_StagedLargeValue), NULL);
    jz_vec_drop(&pending_writes, sizeof(groove_PendingTableWrite), groove__db__PendingTableWrite__drop);
    jz_btree_drop(&exact_keys);
    jz_vec_drop(&accepted_large_values, sizeof(groove_StagedLargeValueId), NULL);
    return e;
}

/* Persist a resident publication while this caller keeps driving the suspended
 * query work it owns. Polling progress here is the owner turn. */
groove_PersistedBatch groove__db__commit__Database__persist_with_progress(groove_Database *db, const groove_AppliedBatch *applied) {
    for (;;) {
        const groove_PersistenceOrder *order = applied->order;
        if (order->has_failure || order->next == applied->publication) break;
        /* A progress failure poisons the database, which the caller observes on
         * its next operation. The publication itself must still settle. */
        if (!groove__db__query__Database__has_pending_progress(db)) break;
        jz_err ignored = JZ_OK;
        if (groove__db__query__Database__poll_progress(db, &NOOP_WAKER, &ignored)) jz_err_drop(&ignored);
    }
    return groove__db__AppliedBatch__persist(applied);
}

/* Install one persistence result and advance only the contiguous durable
 * publication frontier. */
static jz_err finish_persistence_inner(groove_Database *db, groove_PersistedBatch *persistence, groove_PublicationId *out) {
    if (db->publication_persistence != persistence->receipt.order) return groove__db__Error__publication_not_found(persistence->publication);
    groove__db__PersistenceReceipt__finish(&persistence->receipt);
    if (db->last_tick_metrics) groove__ivm__runtime__TickMetrics__drop(db->last_tick_metrics);
    db->last_tick_metrics = persistence->metrics.tick ? groove__ivm__runtime__TickMetrics__clone(persistence->metrics.tick) : NULL;
    if (db->has_last_commit_metrics) groove__db__CommitMetrics__drop(&db->last_commit_metrics);
    groove__db__CommitMetrics__Clone__clone(&db->last_commit_metrics, &persistence->metrics);
    db->has_last_commit_metrics = true;
    if (persistence->result.code) {
        if (persistence->notifications_deferred) groove__ivm__runtime__IvmRuntime__discard_deferred_notifications(db->ivm_runtime, persistence->publication);
        db->poisoned = true;
        jz_err error = persistence->result;
        persistence->result = JZ_OK;
        return error;
    }
    if (!jz_btree_contains(&db->resident_publications, &persistence->publication)) return groove__db__Error__publication_not_found(persistence->publication);
    jz_btree_insert(&db->persisted_publications, &persistence->publication, NULL, NULL);
    u64 frontier = db->has_durable_publication_frontier
                       ? (db->durable_publication_frontier == UINT64_MAX ? UINT64_MAX : db->durable_publication_frontier + 1)
                       : 1;
    while (jz_btree_remove(&db->persisted_publications, &frontier, NULL, NULL)) {
        void *state;
        if (jz_btree_remove(&db->resident_publications, &frontier, NULL, &state)) jz_rc_release(state);
        jz_btree_remove(&db->large_value_lifecycle_publications, &frontier, NULL, NULL);
        db->has_durable_publication_frontier = true;
        db->durable_publication_frontier = frontier;
        frontier = frontier == UINT64_MAX ? UINT64_MAX : frontier + 1;
    }
    if (db->large_value_lifecycle_publications.len == 0) {
        groove_LifecycleMutex *guard = db->large_value_publication_lifecycle_guard;
        db->large_value_publication_lifecycle_guard = NULL;
        *db->large_value_lifecycle_held = false;
        lifecycle_guard_drop(guard);
    }
    groove__db__commit__Database__refresh_resident_writes(db);
    if (persistence->notifications_deferred) groove__ivm__runtime__IvmRuntime__settle_deferred_notifications(db->ivm_runtime, persistence->publication);
    *out = persistence->publication;
    return JZ_OK;
}
jz_err groove__db__commit__Database__finish_persistence(groove_Database *db, groove_PersistedBatch persistence, groove_PublicationId *out) {
    jz_err result = finish_persistence_inner(db, &persistence, out);
    groove__db__PersistedBatch__drop(&persistence);
    if (result.code || db->resident_publications.len == 0) groove__db__wake_publication_owner(db->publication_persistence);
    return result;
}

/* ---- pending writes ---- */
static jz_err pending_write_from_owned_operation(const groove_Database *db, groove_BatchOperation operation, groove_PendingTableWrite *out);

jz_err groove__db__commit__Database__pending_writes_from_batch(const groove_Database *db, groove_DatabaseBatch *batch, groove_PendingTableWriteVec *out) {
    groove_PreparedBatchWrites prepared = batch->prepared;
    memset(&batch->prepared, 0, sizeof batch->prepared);
    groove_PendingTableWriteVec pending_writes = {0};
    if (prepared.owner && prepared.owner == db->batch_preparation_owner) pending_writes = prepared.writes;
    else jz_vec_drop(&prepared.writes, sizeof(groove_PendingTableWrite), groove__db__PendingTableWrite__drop);
    if (prepared.owner) jz_rc_release(prepared.owner);
    u32 prepared_count = pending_writes.len;
    groove_BatchOperationVec operations = batch->operations;
    memset(&batch->operations, 0, sizeof batch->operations);
    jz_vec_reserve(&pending_writes, operations.len - prepared_count, sizeof(groove_PendingTableWrite));
    for (u32 i = 0; i < prepared_count && i < operations.len; i++) groove__db__batch__BatchOperation__drop(&operations.ptr[i]);
    jz_err e = JZ_OK;
    u32 i = prepared_count;
    for (; i < operations.len; i++) {
        groove_PendingTableWrite pending;
        e = pending_write_from_owned_operation(db, operations.ptr[i], &pending);
        if (e.code) { i++; break; }
        JZ_VEC_PUSH(pending_writes, pending);
    }
    for (; i < operations.len; i++) groove__db__batch__BatchOperation__drop(&operations.ptr[i]);
    jz_vec_drop(&operations, sizeof(groove_BatchOperation), NULL);
    if (e.code) { jz_vec_drop(&pending_writes, sizeof(groove_PendingTableWrite), groove__db__PendingTableWrite__drop); return e; }
    *out = pending_writes;
    return JZ_OK;
}

void groove__db__commit__Database__refresh_resident_writes(groove_Database *db) {
    groove__storage__StagedWriteState__reset(db->resident_writes);
    u64 *k; void **state;
    JZ_BTREE_FOR(&db->resident_publications, k, state)
        groove__storage__StagedWriteState__extend_shared(db->resident_writes, groove__storage__StagedWriteState__snapshot(*state));
}

jz_err groove__db__commit__Database__ensure_batch_storage_txn(const groove_Database *db, groove_DatabaseBatch *batch) {
    groove_PreparedBatchWrites *prepared = &batch->prepared;
    groove_StagedWriteState *txn_operations = groove__db__batch__DatabaseBatch__txn_operations(batch);
    if (!(prepared->owner && prepared->owner == db->batch_preparation_owner)) {
        if (prepared->owner) jz_rc_release(prepared->owner);
        prepared->owner = jz_rc_clone(db->batch_preparation_owner);
        jz_vec_truncate(&prepared->writes, 0, sizeof(groove_PendingTableWrite), groove__db__PendingTableWrite__drop);
        groove__storage__StagedWriteState__reset(txn_operations);
        batch->txn_indexed_operations = 0;
    }
    while (batch->txn_indexed_operations < batch->operations.len) {
        const groove_BatchOperation *operation = &batch->operations.ptr[batch->txn_indexed_operations];
        groove_PendingTableWrite pending;
        JZ_TRY(groove__db__commit__Database__pending_write_from_operation(db, operation, &pending));
        groove_OwnedWriteOperation op;
        jz_err e = groove__db__commit__Database__owned_storage_operation_for_pending(db, &pending, &op);
        if (e.code) { groove__db__PendingTableWrite__drop(&pending); return e; }
        groove__storage__StagedWriteState__stage(txn_operations, op);
        JZ_VEC_PUSH(prepared->writes, pending);
        batch->txn_indexed_operations += 1;
    }
    return JZ_OK;
}

jz_err groove__db__commit__Database__owned_storage_operation_for_pending(const groove_Database *db, const groove_PendingTableWrite *pending, groove_OwnedWriteOperation *out) {
    out->cf = jz_str_clone(&pending->table);
    out->key = jz_bytes_clone(&pending->key);
    if (pending->tag == GROOVE_PENDING_SET) {
        out->tag = GROOVE_WRITE_OP_SET;
        if (!groove__db__PendingTableWrite__stored_record(pending, &out->value)) jz_panic("set has a stored record");
    } else {
        out->tag = GROOVE_WRITE_OP_DELETE;
        memset(&out->value, 0, sizeof out->value);
    }
    return JZ_OK;
}

/* ---- descriptors ---- */
static jz_err record_descriptor(const groove_Database *db, const char *table, u32 tn, u32 variant_tag, groove_RecordDescriptor *out) {
    const groove_RecordDescriptor *d = groove__ivm__runtime__IvmRuntime__record_descriptor(db->ivm_runtime, table, tn, variant_tag);
    if (d) { *out = *d; return JZ_OK; }
    const groove_TableSchema *schema;
    JZ_TRY(groove__db__commit__Database__table(db, table, tn, &schema));
    if (!groove__schema__TableSchema__record_schema_for_variant(schema, variant_tag, out))
        return groove__db__Error__table_version(GROOVE_DB_UnknownTableVariant, table, tn, variant_tag);
    return JZ_OK;
}
static jz_err record_descriptor_for_input(const groove_Database *db, const jz_str *table, const groove_RecordInput *record, groove_RecordDescriptor *out) {
    u32 variant_tag = record->tag == GROOVE_RECORD_INPUT_VALUES ? 0 : record->record.variant_tag;
    return record_descriptor(db, (const char *)table->ptr, table->len, variant_tag, out);
}
static jz_err record_descriptor_for_raw_input(const groove_Database *db, const jz_str *table, const groove_RawRecordInput *record, groove_RecordDescriptor *out) {
    u32 variant_tag = record->tag == GROOVE_RAW_INPUT_PAYLOAD ? 0 : record->record.variant_tag;
    return record_descriptor(db, (const char *)table->ptr, table->len, variant_tag, out);
}

static groove_PendingTableWrite pending_set(u8 mode, jz_str table, jz_bytes key, const groove_ResolvedRecord *r) {
    groove_PendingTableWrite w = { GROOVE_PENDING_SET, mode, table, key, r->variant_tag, r->descriptor, r->record };
    return w;
}
static u8 mode_for(u8 tag) {
    switch (tag) {
    case GROOVE_BATCH_INSERT: case GROOVE_BATCH_INSERT_RAW: return GROOVE_WRITE_MODE_INSERT;
    case GROOVE_BATCH_INSERT_RAW_FRESH: return GROOVE_WRITE_MODE_INSERT_FRESH;
    default: return GROOVE_WRITE_MODE_UPDATE;
    }
}

jz_err groove__db__commit__Database__pending_write_from_operation(const groove_Database *db, const groove_BatchOperation *op, groove_PendingTableWrite *out) {
    const jz_str *table = &op->table;
    const groove_TableSchema *table_schema;
    JZ_TRY(groove__db__commit__Database__table(db, (const char *)table->ptr, table->len, &table_schema));
    groove_RecordDescriptor descriptor;
    groove_ResolvedRecord r;
    switch (op->tag) {
    case GROOVE_BATCH_INSERT:
    case GROOVE_BATCH_UPDATE: {
        JZ_TRY(record_descriptor_for_input(db, table, &op->record, &descriptor));
        JZ_TRY(groove__db__encoding__resolve_record_input(table_schema, &op->record, descriptor, &r));
        jz_bytes key;
        jz_err e = groove__db__encoding__primary_key_bytes(table_schema, r.variant_tag, r.descriptor, r.record.ptr, r.record.len, &key);
        if (e.code) { jz_bytes_drop(&r.record); return e; }
        *out = pending_set(mode_for(op->tag), jz_str_clone(table), key, &r);
        return JZ_OK;
    }
    case GROOVE_BATCH_INSERT_RAW:
    case GROOVE_BATCH_INSERT_RAW_FRESH:
    case GROOVE_BATCH_UPDATE_RAW: {
        JZ_TRY(record_descriptor_for_raw_input(db, table, &op->raw, &descriptor));
        JZ_TRY(groove__db__encoding__resolve_raw_record_input(table_schema, &op->raw, descriptor, &r));
        groove_PrimaryKeyValue key_value;
        groove__db__batch__PrimaryKeyValue__Clone__clone(&key_value, &op->key);
        jz_bytes key = groove__db__batch__PrimaryKeyValue__into_bytes(key_value);
        jz_err e = groove__db__encoding__validate_raw_primary_key(table_schema, r.variant_tag, r.descriptor, r.record.ptr, r.record.len, key.ptr, key.len);
        if (e.code) { jz_bytes_drop(&key); jz_bytes_drop(&r.record); return e; }
        *out = pending_set(mode_for(op->tag), jz_str_clone(table), key, &r);
        return JZ_OK;
    }
    default: {
        groove_PrimaryKeyValue key_value;
        groove__db__batch__PrimaryKeyValue__Clone__clone(&key_value, &op->key);
        groove_PendingTableWrite w = { GROOVE_PENDING_DELETE, 0, jz_str_clone(table), groove__db__batch__PrimaryKeyValue__into_bytes(key_value), 0,
                                       groove__schema__TableSchema__record_schema(table_schema), {0} };
        *out = w;
        return JZ_OK;
    }
    }
}

static jz_err pending_write_from_owned_operation(const groove_Database *db, groove_BatchOperation op, groove_PendingTableWrite *out) {
    const groove_TableSchema *table_schema;
    groove_RecordDescriptor descriptor;
    groove_ResolvedRecord r;
    jz_err e = groove__db__commit__Database__table(db, (const char *)op.table.ptr, op.table.len, &table_schema);
    if (e.code) { groove__db__batch__BatchOperation__drop(&op); return e; }
    switch (op.tag) {
    case GROOVE_BATCH_INSERT:
    case GROOVE_BATCH_UPDATE: {
        e = record_descriptor_for_input(db, &op.table, &op.record, &descriptor);
        if (e.code) { groove__db__batch__BatchOperation__drop(&op); return e; }
        e = groove__db__encoding__resolve_owned_record_input(table_schema, op.record, descriptor, &r);
        if (e.code) { jz_str_drop(&op.table); return e; }
        jz_bytes key;
        e = groove__db__encoding__primary_key_bytes(table_schema, r.variant_tag, r.descriptor, r.record.ptr, r.record.len, &key);
        if (e.code) { jz_str_drop(&op.table); jz_bytes_drop(&r.record); return e; }
        *out = pending_set(mode_for(op.tag), op.table, key, &r);
        return JZ_OK;
    }
    case GROOVE_BATCH_INSERT_RAW:
    case GROOVE_BATCH_INSERT_RAW_FRESH:
    case GROOVE_BATCH_UPDATE_RAW: {
        e = record_descriptor_for_raw_input(db, &op.table, &op.raw, &descriptor);
        if (e.code) { groove__db__batch__BatchOperation__drop(&op); return e; }
        e = groove__db__encoding__resolve_owned_raw_record_input(table_schema, op.raw, descriptor, &r);
        if (e.code) { jz_str_drop(&op.table); groove__db__batch__PrimaryKeyValue__drop(&op.key); return e; }
        jz_bytes key = groove__db__batch__PrimaryKeyValue__into_bytes(op.key);
        e = groove__db__encoding__validate_raw_primary_key(table_schema, r.variant_tag, r.descriptor, r.record.ptr, r.record.len, key.ptr, key.len);
        if (e.code) { jz_str_drop(&op.table); jz_bytes_drop(&key); jz_bytes_drop(&r.record); return e; }
        *out = pending_set(mode_for(op.tag), op.table, key, &r);
        return JZ_OK;
    }
    default: {
        groove_PendingTableWrite w = { GROOVE_PENDING_DELETE, 0, op.table, groove__db__batch__PrimaryKeyValue__into_bytes(op.key), 0,
                                       groove__schema__TableSchema__record_schema(table_schema), {0} };
        *out = w;
        return JZ_OK;
    }
    }
}

jz_err groove__db__commit__Database__table_storage_descriptor(const groove_Database *db, const char *table, u32 n, groove_RecordDescriptor *out) {
    const groove_RecordDescriptor *d = groove__ivm__runtime__IvmRuntime__table_storage_descriptor(db->ivm_runtime, table, n);
    if (!d) return groove__db__Error__str(GROOVE_DB_TableNotFound, table, n);
    *out = *d;
    return JZ_OK;
}
jz_err groove__db__commit__Database__table(const groove_Database *db, const char *table, u32 n, const groove_TableSchema **out) {
    JZ_TRY(groove__db__commit__Database__ensure_not_poisoned(db));
    const groove_TableSchema *t = groove__ivm__runtime__IvmRuntime__table(db->ivm_runtime, table, n);
    if (!t) return groove__db__Error__str(GROOVE_DB_TableNotFound, table, n);
    *out = t;
    return JZ_OK;
}
jz_err groove__db__commit__Database__ensure_not_poisoned(const groove_Database *db) {
    if (db->poisoned || *db->abandoned_application) return groove__db__Error__unit(GROOVE_DB_DatabasePoisoned);
    return JZ_OK;
}
